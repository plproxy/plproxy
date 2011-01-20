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
#include "parser.tab.h"

/* import standard_conforming_strings */
#if PG_VERSION_NUM >= 80500
#include <parser/parser.h>
#else
#ifndef PGDLLIMPORT
#define PGDLLIMPORT DLLIMPORT
#endif
extern PGDLLIMPORT bool standard_conforming_strings;
#endif

/*
 * Calculare numeric flex version.
 */
#if !defined(YY_FLEX_MAJOR_VERSION) || !defined(YY_FLEX_MINOR_VERSION)
#error Flex required
#endif
#ifndef YY_FLEX_SUBMINOR_VERSION
#define YY_FLEX_SUBMINOR_VERSION 0
#endif
#define FLXVER ((YY_FLEX_MAJOR_VERSION*1000 + YY_FLEX_MINOR_VERSION)*1000 + YY_FLEX_SUBMINOR_VERSION)

/* shut down crappy flex warnings */
#if FLXVER < 2005035
int yyget_lineno(void);
int yyget_leng(void);
FILE *yyget_in(void);
FILE *yyget_out(void);
char *yyget_text(void);
void plproxy_yyset_lineno(int);
void plproxy_yyset_in(FILE *);
void plproxy_yyset_out(FILE *);
int plproxy_yyget_debug(void);
void plproxy_yyset_debug(int);
int plproxy_yylex_destroy(void);
#endif

/* point to parser value */
#define yylval plproxy_yylval

/*
 * Allocate in CurrentMemoryContext.
 *
 * If we want to support flex 2.5.4, we cannot use
 * options noyyalloc, noyyrealloc, noyyfree.
 *
 * Thus such need to hack malloc() et al.
 */

#define malloc palloc
#define realloc repalloc
#define free(p) do { if (p) pfree(p); } while (0)


void plproxy_yylex_startup(void)
{
	/* there may be stale pointers around, drop them */
#if FLXVER < 2005031
	(YY_CURRENT_BUFFER) = NULL;
#else
	(yy_buffer_stack) = NULL;
#endif
	plproxy_yylex_destroy();
}

/*
 * compat stuff for older flex
 */
#if FLXVER < 2005031

/* old flex */

int plproxy_yylex_destroy(void)
{
	plproxy_yy_delete_buffer(YY_CURRENT_BUFFER);
	YY_CURRENT_BUFFER = NULL;
	yy_start = 0;
	yy_init = 1;
	yylineno = 1;
	return 0;
}

int plproxy_yyget_lineno(void)
{
	return yylineno;
}

#endif

/* own error handling */
#define YY_FATAL_ERROR(msg) plproxy_yyerror(msg)

/* disable stdio related code */
#define YY_INPUT(buf, res, maxlen) { res = 0; }

#define YY_NO_INPUT

/* shortcut for returning SQLPART */
#define RETPART do { yylval.str = yytext; return SQLPART; } while (0)

/* dollar quoting helpers */
static void dlr_start(const char *txt);
static bool dlr_stop(const char *txt);

static const char *unquote(const char *qstr, bool std);

%}

%option 8bit case-insensitive
%option warn nodefault yylineno
%option nounput noyywrap never-interactive
%option prefix="plproxy_yy"

/* states */
%x sql
%x qident
%x stdq
%x extq
%x longcom
%x dolq
%x plcom

/* whitespace */
SPACE		[ \t\n\r]

/* sql ident.  include dotted parts also */
WORD		[_a-z\200-\377][a-z0-9_\200-\377]*
IDENT		{WORD}({SPACE}*[.]{SPACE}*{WORD})*

/* argument ref by val: $1 */
NUMIDENT	[$][0-9]+

/* regular int value for hash spec */
PLNUMBER	[0-9]+

/* SQL numeric value */
SQLNUM		[0-9][.0-9]*

/*
 * Symbols that may exist in sql.   They must be matched one-by-one,
 * to avoid conflics with combos.
 *
 * Excludes: [$'";`]
 */
SQLSYM		[-!#%&()*+,/:<=>?@\[\]^{|}~.]

/* Dollar quote ID */
DOLQ_START      [a-z\200-\377_]
DOLQ_CONT       [a-z\200-\377_0-9]
DOLQ		({DOLQ_START}{DOLQ_CONT}*)

%%

	/* PL/Proxy language keywords */

cluster			{ return CLUSTER; }
connect			{ return CONNECT; }
run			{ return RUN; }
on			{ return ON; }
all			{ return ALL; }
any			{ return ANY; }
split		{ return SPLIT; }
target		{ return TARGET; }
select			{ BEGIN(sql); yylval.str = yytext; return SELECT; }

	/* function call */

	/* hack to avoid parsing "SELECT (" as function call */
select{SPACE}*[(]	{ yyless(6); BEGIN(sql); yylval.str = yytext; return SELECT; }
{IDENT}{SPACE}*[(]	{ BEGIN(sql); yylval.str = yytext; return FNCALL; }

	/* PL/Proxy language comments/whitespace */

{SPACE}+		{ }
[-][-][^\n]*		{ }
[/][*]			{ BEGIN(plcom); }
<plcom>[^*/]+		{ }
<plcom>[*]+[^*/]+	{ }
<plcom>[*]+[/]		{ BEGIN(INITIAL); }
<plcom>.		{ }

	/* PL/Proxy non-keyword elements */

{IDENT}			{ yylval.str = yytext; return IDENT; }
{NUMIDENT}		{ yylval.str = yytext; return IDENT; }
{PLNUMBER}		{ yylval.str = yytext; return NUMBER; }
[']([^']+|[']['])*[']	{ yylval.str = unquote(yytext, true); return STRING; }

	/* unparsed symbol, let parser decide */

.			{ return *(yytext); }

	/*
	 * Following is parser for SQL statements.
	 */

	/* SQL line comment */

<sql>[-][-][^\n]*	{ /* \n will be parsed as whitespace */ }

	/* C comment, parse it as whitespace */

<sql>[/][*]		{ BEGIN(longcom); }
<longcom>[^*/]+		{ }
<longcom>[*]+[^*/]+	{ }
<longcom>[*]+[/]	{ BEGIN(sql); yylval.str = " "; return SQLPART; }
<longcom>.		{ }

	/* Dollar quoted string */

<sql>[$]{DOLQ}?[$]	{ BEGIN(dolq); dlr_start(yytext); RETPART; }
<dolq>[^$]+		{ RETPART; }
<dolq>[$]{DOLQ}?[$]	{ if (dlr_stop(yytext)) { BEGIN(sql); RETPART; }
			  /* if wrong one, report only 1 char */
			  else { yyless(1); RETPART; } }
<dolq>[$][^$]*		{ RETPART; }

	/* quoted indentifier */

<sql>["]		{ BEGIN(qident); RETPART; }
<qident>[^"]+		{ RETPART; }
<qident>[\\].		{ RETPART; }
<qident>["]		{ BEGIN(sql); RETPART; }

	/* quoted string start */

<sql>E[']		{ BEGIN(extq); RETPART; }
<sql>[']		{ if (standard_conforming_strings)
			    BEGIN(stdq); else BEGIN(extq);
			  RETPART; }

	/* SQL standard quoted string body */

<stdq>[^']+		{ RETPART; }
<stdq>['][']		{ RETPART; }
<stdq>[']		{ BEGIN(sql); RETPART; }

	/* extended quoted string body */

<extq>[^'\\]+		{ RETPART; }
<extq>['][']		{ RETPART; }
<extq>[\\].		{ RETPART; }
<extq>[']		{ BEGIN(sql); RETPART; }
<extq>.			{ RETPART; }

	/* SQL identifier */

<sql>{IDENT}		{ yylval.str = yytext; return SQLIDENT; }

	/* $x argument reference */

<sql>{NUMIDENT}		{ yylval.str = yytext; return SQLIDENT; }

	/* SQL number */

<sql>{SQLNUM}		{ RETPART; }

	/* SQL symbol, parse them one-by-one */

<sql>{SQLSYM}		{ RETPART; }

	/* compress whitespace to singe ' ' */

<sql>{SPACE}+		{ yylval.str = " "; return SQLPART; }

	/* SQL statement end */

<sql>[;]		{ BEGIN(INITIAL); return *(yytext); }

	/* unparsed symbol, let the parser error out */

<sql>.			{ return *(yytext); }

%%

static char *dlr_token = NULL;

/* remember dollar quote name */
static void dlr_start(const char *txt)
{
	dlr_token = pstrdup(txt);
}

/* check if matches stored name */
static bool dlr_stop(const char *txt)
{
	bool res =  strcmp(txt, dlr_token) == 0;
	if (res) {
		pfree(dlr_token);
		dlr_token = NULL;
	}
	return res;
}

static const char *unquote(const char *qstr, bool std)
{
	const char *p;
	StringInfoData buf;

	initStringInfo(&buf);
	for (p = qstr + 1; *p; p++) {
		if (*p == '\'') {
			if (*++p == 0)
				break;
			appendStringInfoChar(&buf, *p);
		} else
			appendStringInfoChar(&buf, *p);
	}

	if (0) yy_fatal_error("avoid unused func warning");

	/* leak buf.data */
	return buf.data;
}

