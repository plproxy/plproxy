
wiki = https://developer.skype.com/SkypeGarage/DbProjects/PlProxy

web = mkz@shell.pgfoundry.org:/home/pgfoundry.org/groups/plproxy/htdocs/

ASCIIDOC = asciidoc

HTML = config.html syntax.html todo.html tutorial.html faq.html

all:

html: $(HTML)

clean:
	rm -f *.html

web: $(HTML)
	rsync -avz $(HTML) $(web)/doc/

%.html: %.txt
	$(ASCIIDOC) -a toc $<

