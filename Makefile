protoc:
	protoc -I ./pb risk_monitor_server.proto --go_out=plugins=grpc:./pb

build:
	GOOS=linux go build -o app
	docker build -t docker-registry-default.apps.dev-cefcfco.com/rma-7x24/risk-message-deliver:latest .
	rm -f app

push:
	oc login console.dev-cefcfco.com:8443 -u admin -p 123.123a 
	docker login -u admin -p `oc whoami -t` docker-registry-default.apps.dev-cefcfco.com
	docker push docker-registry-default.apps.dev-cefcfco.com/rma-7x24/risk-message-deliver:latest