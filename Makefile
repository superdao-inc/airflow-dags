run-local:
	docker-compose -f deployment/local/docker-compose.yaml up -d --build
	/bin/bash deployment/local/add_connections_prod.sh

down:
	 docker-compose -f deployment/local/docker-compose.yaml down --volumes --remove-orphans

delete:
	docker-compose -f deployment/local/docker-compose.yaml down --volumes --remove-orphans --rmi all
