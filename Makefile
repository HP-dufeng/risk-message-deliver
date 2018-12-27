protoc:
	protoc -I ./pb risk_monitor_server.proto --go_out=plugins=grpc:./pb

build:
	GOOS=linux go build -o app
	docker build -t docker-registry-default.apps.dev-cefcfco.com/rma/risk-message-deliver:latest .
	rm -f app

push:
	oc login console.dev-cefcfco.com:8443 -u admin -p 123.123a 
	docker login -u admin -p `oc whoami -t` docker-registry-default.apps.dev-cefcfco.com
	docker tag docker-registry-default.apps.dev-cefcfco.com/rma/risk-message-deliver:latest docker-registry-default.apps.dev-cefcfco.com/rma/risk-message-deliver:v2.0.0
	docker push docker-registry-default.apps.dev-cefcfco.com/rma/risk-message-deliver:v2.0.0