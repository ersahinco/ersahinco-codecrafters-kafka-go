BROKER=go run app/main.go /tmp/server.properties

run:
	$(BROKER)

test-dtp:
	python3 tests/test_describe_partitions.py

props:
	mkdir -p /tmp
	cat > /tmp/server.properties <<'EOF'
topic.alpha.id=11111111-2222-3333-4444-555555555555
topic.alpha.partitions=2
topic.beta.id=aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee
topic.beta.partitions=3
EOF
	@cat /tmp/server.properties
