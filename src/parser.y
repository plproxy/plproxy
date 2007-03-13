%{
/*
 * PL/Proxy - easy access to partitioned database.
 * 
 * Copyright (c) 2006 Sven Suursoho, Skype Technologies OÜ
 * Copyright (c) 2007 Marko Kreen, Skype Technologies OÜ
 * 
 * Permission to use, copy, modify, and distribute this software for any
 * purpose with or without fee is hereby granted, provided that the above
 * copyright notice and this permission notice appear in all copies.
 * 
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR
 * ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
 * WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
 * ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
 * OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 */

#include "plproxy.h"
#include "scanner.h"

/* avoid permanent allocations */
#define YYSTACK_USE_ALLOCA 1
/* remove unused code */
#define YY_LOCATION_PRINT(File, Loc) (0)
#define YY_(x) (x)

/* during parsing, keep reference to function here */
static ProxyFunction *xfunc;

/* remember what happened */
static int got_run, got_cluster, got_connect;

static QueryBuffer *cluster_sql;
static QueryBuffer *select_sql;
static QueryBuffer *hash_sql;
static QueryBuffer *cur_sql;

%}

%name-prefix="plproxy_yy"

%token <str> CONNECT CLUSTER RUN ON ALL ANY SELECT
%token <str> IDENT CONST NUMBER FNCALL STRING
%token <str> SQLIDENT SQLPART

%union
{
	const char *str;
}

%%

body: | body stmt ;

stmt: cluster_stmt | run_stmt | select_stmt | connect_stmt ;

connect_stmt: CONNECT connect_spec ';'	{
					if (got_connect)
						yyerror("Only one CONNECT statement allowed");
					xfunc->run_type = R_EXACT;
					got_connect = 1; }
			;

connect_spec: STRING	{ xfunc->connect_str = plproxy_func_strdup(xfunc, $1); }
			;

cluster_stmt: CLUSTER cluster_spec ';' {
							if (got_cluster)
								yyerror("Only one CLUSTER statement allowed");
							got_cluster = 1; }
			;

cluster_spec: cluster_name | cluster_func sql_token_list
			;

cluster_func: FNCALL	{ cluster_sql = plproxy_query_start(xfunc, false);
						  cur_sql = cluster_sql;
						  plproxy_query_add_const(cur_sql, "select ");
						  plproxy_query_add_const(cur_sql, $1); }
			;

cluster_name: STRING	{ xfunc->cluster_name = plproxy_func_strdup(xfunc, $1); }
			;

run_stmt: RUN ON run_spec ';'	{ if (got_run)
									yyerror("Only one RUN statement allowed");
								  got_run = 1; }
		;

run_spec: hash_func sql_token_list	{ xfunc->run_type = R_HASH; }
		| NUMBER					{ xfunc->run_type = R_EXACT; xfunc->exact_nr = atoi($1); }
		| ANY						{ xfunc->run_type = R_ANY; }
		| ALL						{ xfunc->run_type = R_ALL; }
		;

hash_func: FNCALL	{ hash_sql = plproxy_query_start(xfunc, false);
	 				  cur_sql = hash_sql;
	 				  plproxy_query_add_const(cur_sql, "select * from ");
	 				  plproxy_query_add_const(cur_sql, $1); }
		 ;

select_stmt: sql_start sql_token_list ';' ;

sql_start: SELECT		{ if (select_sql)
							yyerror("Only one SELECT statement allowed");
						  select_sql = plproxy_query_start(xfunc, true);
						  cur_sql = select_sql;
						  plproxy_query_add_const(cur_sql, $1); }
		 ;
sql_token_list: sql_token
			  | sql_token_list sql_token
		      ;
sql_token: SQLPART		{ plproxy_query_add_const(cur_sql, $1); }
		 | SQLIDENT		{ if (!plproxy_query_add_ident(cur_sql, $1))
							yyerror("invalid argument reference: %s", $1); }
		 ;

%%

/*
 * report parser error.
 */
void yyerror(const char *fmt, ...)
{
	char buf[1024];
	int lineno = plproxy_yyget_lineno();
	va_list ap;

	va_start(ap, fmt);
	vsnprintf(buf, sizeof(buf), fmt, ap);
	va_end(ap);

	/* reinitialize scanner */
	plproxy_yylex_destroy();

	plproxy_error(xfunc, "Compile error at line %d: %s", lineno, buf);
}


/* actually run the flex/bison parser */
void plproxy_run_parser(ProxyFunction *func, const char *body, int len)
{
	xfunc = func;
	got_run = got_cluster = got_connect = 0;

	cur_sql = select_sql = NULL;

	/* setup scanner */
	plproxy_yy_scan_bytes(body, len);

	/* run parser */
	yyparse();

	/* check for mandatory statements */
	if (got_connect) {
		if (got_cluster || got_run)
			yyerror("CONNECT cannot be used with CLUSTER/RUN");
	} else {
		if (!got_cluster)
			yyerror("CLUSTER statement missing");
		if (!got_run)
			yyerror("RUN statement missing");
	}

	/* reinitialize scanner */
	plproxy_yylex_destroy();

	/* copy hash data if needed */
	if (xfunc->run_type == R_HASH)
		xfunc->hash_sql = plproxy_query_finish(hash_sql);

	/* store sql */
	if (select_sql)
		xfunc->remote_sql = plproxy_query_finish(select_sql);
	
	if (cluster_sql)
		xfunc->cluster_sql = plproxy_query_finish(cluster_sql);

	xfunc = NULL;
}

