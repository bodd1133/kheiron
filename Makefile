REPO=rlb-tech-test

deploy-image:
	virtualenv ./.venv; source ./.venv/bin/activate; 
	$(eval URI := $(shell aws ecr describe-repositories --repository-names $(REPO) | jq -r '.repositories[0] | .repositoryUri'))
	aws ecr get-login-password | docker login --password-stdin --username AWS $(URI)
	cd python && docker build . -t $(REPO)-agent
	docker tag $(REPO)-agent:latest $(URI):latest
	docker push $(URI):latest

test: 
	virtualenv ./.venv; source ./.venv/bin/activate; \
	cd python && pip3 install -r ./requirements.txt; pytest ./test/

test-clean:
	rm -rf ./.venv .pytest_cache; cd python; rm -rf __pycache__ .pytest_cache; cd test; rm -rf __pycache__ .pytest_cache
