defmodule RabbitMQStreamTest.MetadataResolver do
  use ExUnit.Case, async: true

  alias RabbitMQStream.Connection.MetadataResolver
  alias RabbitMQStream.Message.Types.QueryMetadataResponseData
  alias RabbitMQStream.Message.Types.QueryMetadataResponseData.{StreamData, BrokerData}

  @moduletag :v3_11
  @moduletag :v3_12
  @moduletag :v3_13

  @leader %BrokerData{reference: 0, host: "node-a", port: 5552}
  @replica1 %BrokerData{reference: 1, host: "node-b", port: 5552}
  @replica2 %BrokerData{reference: 2, host: "node-c", port: 5552}

  test "resolves the leader and replicas of an existing stream" do
    metadata = %QueryMetadataResponseData{
      brokers: [@leader, @replica1, @replica2],
      streams: [%StreamData{code: :ok, name: "my-stream", leader: 0, replicas: [1, 2]}]
    }

    assert {:ok, %{leader: @leader, replicas: [@replica1, @replica2]}} =
             MetadataResolver.resolve(metadata, "my-stream")
  end

  test "resolves a stream with no replicas" do
    metadata = %QueryMetadataResponseData{
      brokers: [@leader],
      streams: [%StreamData{code: :ok, name: "my-stream", leader: 0, replicas: []}]
    }

    assert {:ok, %{leader: @leader, replicas: []}} = MetadataResolver.resolve(metadata, "my-stream")
  end

  test "drops replica references that aren't in the brokers list, without failing" do
    metadata = %QueryMetadataResponseData{
      brokers: [@leader, @replica1],
      streams: [%StreamData{code: :ok, name: "my-stream", leader: 0, replicas: [1, 99]}]
    }

    assert {:ok, %{leader: @leader, replicas: [@replica1]}} = MetadataResolver.resolve(metadata, "my-stream")
  end

  test "returns an error when the stream isn't present in the response" do
    metadata = %QueryMetadataResponseData{
      brokers: [@leader],
      streams: [%StreamData{code: :ok, name: "other-stream", leader: 0, replicas: []}]
    }

    assert {:error, :stream_not_found} = MetadataResolver.resolve(metadata, "my-stream")
  end

  test "returns an error when the broker reports the stream doesn't exist" do
    metadata = %QueryMetadataResponseData{
      brokers: [],
      streams: [%StreamData{code: :stream_does_not_exist, name: "my-stream", leader: 0, replicas: []}]
    }

    assert {:error, :stream_not_found} = MetadataResolver.resolve(metadata, "my-stream")
  end

  test "returns an error when the leader reference can't be resolved against the brokers list" do
    metadata = %QueryMetadataResponseData{
      brokers: [@replica1],
      streams: [%StreamData{code: :ok, name: "my-stream", leader: 0, replicas: [1]}]
    }

    assert {:error, :leader_not_found} = MetadataResolver.resolve(metadata, "my-stream")
  end
end
