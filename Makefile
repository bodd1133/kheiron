REPO=rlb-tech-test

deploy-image:
	$(eval URI := $(shell aws ecr describe-repositories --repository-names $(REPO) | jq -r '.repositories[0] | .repositoryUri'))
	aws ecr get-login-password | docker login --password-stdin --username AWS $(URI)
	cd python && docker build . -t $(REPO)-agent
	docker tag $(REPO)-agent:latest $(URI):latest
	docker push $(URI):latest

