
wiki = https://developer.skype.com/SkypeGarage/DbProjects/PlProxy

web = mkz@shell.pgfoundry.org:/home/pgfoundry.org/groups/plproxy/htdocs/

ASCIIDOC = asciidoc

HTML = config.html syntax.html todo.html tutorial.html

all:

html: $(HTML)

upload:
	devupload.sh overview.txt $(wiki)
	#devupload.sh config.txt $(wiki)/ClusterConfig
	#devupload.sh syntax.txt $(wiki)/LanguageSyntax
	#devupload.sh todo.txt $(wiki)/ToDo

clean:
	rm -rf api

web: $(HTML)
	rsync -avz $(HTML) $(web)/doc/

%.html: %.txt
	$(ASCIIDOC) -a toc $<

