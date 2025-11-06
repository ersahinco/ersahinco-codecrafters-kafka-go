.PHONY: run test props clean

run:
	./your_program.sh /tmp/server.properties

test:
	codecrafters test

props:
	@mkdir -p /tmp
	@printf 'topic.alpha.id=11111111-2222-3333-4444-555555555555\ntopic.alpha.partitions=2\ntopic.beta.id=aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee\ntopic.beta.partitions=3\n' > /tmp/server.properties
	@echo "Created /tmp/server.properties"
	@cat /tmp/server.properties

clean:
	@rm -f /tmp/server.properties
	@echo "Cleaned up temporary files"

show-topics:
	@if [ -f /tmp/server.properties ]; then \
		echo "Topics in /tmp/server.properties:"; \
		grep "^topic\." /tmp/server.properties | sort; \
	else \
		echo "No properties file found at /tmp/server.properties"; \
	fi
