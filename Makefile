PATH := ./redis-git/src:${PATH}



help:
	@echo "Please use 'make <target>' where <target> is one of"
	@echo "  clean           remove temporary files created by build tools"
	@echo "  cleanmeta       removes all META-* and egg-info/ files created by build tools"	
	@echo "  cleancov        remove all files related to coverage reports"
	@echo "  cleanall        all the above + tmp files from development tools"
	@echo "  test            run test suite"
	@echo "  sdist           make a source distribution"
	@echo "  bdist           make a wheel distribution"
	@echo "  install         install package"
	@echo "  local           build Cython extensions in place"
	@echo " *** CI Commands ***"
	@echo "  test            starts/activates the test cluster nodes and runs tox test"
	@echo "  tox             run all tox environments and combine coverage report after"

clean:
	-rm -f MANIFEST
	-rm -rf dist/
	-rm -rf build/

cleancov:
	-rm -rf htmlcov/
	-coverage combine
	-coverage erase

cleanmeta:
	-rm -rf hbom.egg-info/

cleanall: clean cleancov cleanmeta
	-find . -type f -name "*~" -exec rm -f "{}" \;
	-find . -type f -name "*.orig" -exec rm -f "{}" \;
	-find . -type f -name "*.rej" -exec rm -f "{}" \;
	-find . -type f -name "*.pyc" -exec rm -f "{}" \;
	-find . -type f -name ".redis*" -exec rm -f "{}" \;
	-rm -rf .tox/

sdist: cleanmeta
	python setup.py sdist

bdist: cleanmeta
	python setup.py bdist_wheel

install:
	python setup.py install

local:
	python setup.py build_ext --inplace

test:
	make tox

tox:
	coverage erase
	tox
	coverage report

.PHONY: test
