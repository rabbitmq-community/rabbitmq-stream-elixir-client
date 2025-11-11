# Contributing

Thank you for contributing to the project. We'd love to see your
issues and pull requests.

If you're creating a pull request, please consider these suggestions:

Fork, then clone the repo:

    git clone git@github.com:your-username/rabbitmq-stream.git

Install the dependencies:

    mix deps.get

The tests uses the following default credentials:

    username: guest
    password: guest
    vhost:    /
    port:     5552

There's a `docker-compose.yaml` in the `services` folder.

    docker compose --project-directory services up -d rabbitmq_stream_3_13

You can activate RabbitMQ Stream Plugin on your server, if not already active, with:

    rabbitmq-plugins enable rabbitmq_stream

Make sure the tests pass:

    mix test

Make your change. Add tests for your change. Make the tests pass:

    mix test

To run the Clustered test, run:

    # Start the cluster
    docker-compose --project-directory services/cluster up -d
    # or for proxied
    # docker-compose --project-directory services/proxied-cluster up -d

    docker run --network cluster_rabbitmq -v "${PWD}:/home/rabbitmq-stream" --rm -it --entrypoint bash elixir:1.17.2-otp-26

    cd /home/rabbitmq-stream

    mix local.rebar --force
    mix local.hex --force
    mix deps.unlock --all
    mix deps.get
    mix deps.compile
    mix compile

    # Run the tests
    mix test --exclude test --include v3_13_cluster

    <!-- docker run --network proxied-cluster_rabbitmq -v "${PWD}:/home/rabbitmq-stream" -P elixir:1.17.2-otp-26 /bin/sh -c 'cd /home/rabbitmq-stream; ./services/cluster/test.sh' -->

    <!-- docker run --network cluster_rabbitmq -v "${PWD}:/home/rabbitmq-stream" --rm -it --entrypoint bash elixir:1.17.2-otp-26-->
