defmodule RabbitMQStream.OsirisChunk do
  @moduledoc """
    Struct that holds the data of a Osiris chunk, which is the format used by RabbitMQ Stream to store the stream data.

    You can find more information at their [Github repo](https://github.com/rabbitmq/osiris)

  """
  @type chunk_type :: :chunk_user | :chunk_track_delta | :chunk_track_snapshot
  @spec decode_entry!(type :: chunk_type(), data :: binary()) :: {binary() | [ChunkTrackSnapshot.t()], binary()}

  @type t :: %__MODULE__{
          # <<chunk_type::integer-size(8)>>
          chunk_type: chunk_type(),
          # <<num_entries::unsigned-integer-size(16)>>
          num_entries: non_neg_integer(),
          # <<num_records::unsigned-integer-size(32)>>
          num_records: non_neg_integer(),
          # <<timestamp::integer-size(64)>>
          timestamp: integer(),
          # <<epoch::unsigned-integer-size(64)>>
          epoch: non_neg_integer(),
          # <<chunk_id::unsigned-integer-size(64)>>
          chunk_id: non_neg_integer(),
          # <<chunk_crc::integer-size(32)>>
          chunk_crc: integer(),
          # <<data_length::unsigned-integer-size(32)>>
          data_length: non_neg_integer(),
          # <<trailer_length::unsigned-integer-size(32)>>
          trailer_length: non_neg_integer(),
          # <<reserved::unsigned-integer-size(32)>>
          data_entries: [binary()] | [ChunkTrackSnapshot.t()],
          trailer_entries: [binary()] | nil
        }
  @enforce_keys [
    :chunk_type,
    :num_entries,
    :num_records,
    :timestamp,
    :epoch,
    :chunk_id,
    :chunk_crc,
    :data_length,
    :trailer_length,
    :data_entries
  ]
  defstruct [
    :chunk_type,
    :num_entries,
    :num_records,
    :timestamp,
    :epoch,
    :chunk_id,
    :chunk_crc,
    :data_length,
    :trailer_length,
    :data_entries,
    trailer_entries: nil
  ]

  @magic 0x5
  @version 0x0

  defmodule ChunkTrackSnapshot do
    @moduledoc false
    @type t :: %__MODULE__{
            type: :sequence | :offset,
            id: binary(),
            chunk_id: non_neg_integer() | nil,
            sequence: non_neg_integer() | nil,
            offset: non_neg_integer() | nil
          }
    @enforce_keys [:type, :id]
    defstruct [:type, :id, :chunk_id, :sequence, :offset]

    def decode_entries!(<<
          0::unsigned-integer-size(8),
          id_size::unsigned-integer-size(8),
          id::binary-size(id_size),
          chunk_id::unsigned-integer-64,
          sequence::unsigned-integer-size(64),
          rest::binary
        >>) do
      {[%ChunkTrackSnapshot{type: :sequence, id: id, chunk_id: chunk_id, sequence: sequence}], rest}
    end

    def decode_entries!(<<
          1::unsigned-integer-size(8),
          id_size::unsigned-integer-size(8),
          id::binary-size(id_size),
          offset::integer-size(64),
          rest::binary
        >>) do
      {[%ChunkTrackSnapshot{type: :offset, id: id, offset: offset}], rest}
    end
  end

  defmodule SubBatchEntry do
    @moduledoc false
    @type t :: %__MODULE__{
            # <<compression::integer-size(3)>>
            compression: integer(),
            # <<reserved::integer-size(4)>>
            reserved: integer(),
            # <<num_records::unsigned-integer-size(16)>>
            num_records: non_neg_integer(),
            # <<uncompressed_length::unsigned-integer-size(32)>>
            uncompressed_length: non_neg_integer(),
            # <<length::unsigned-integer-size(32)>>
            length: non_neg_integer(),
            # <<length::binary-size(length)>>
            body: binary()
          }

    @enforce_keys [:compression, :reserved, :num_records, :uncompressed_length, :length, :body]
    defstruct [:compression, :reserved, :num_records, :uncompressed_length, :length, :body]
  end

  defp decode_entry!(:chunk_user, <<0::1, length::unsigned-integer-size(31), rest::binary>>) do
    <<data::binary-size(^length), rest::binary>> = rest
    {data, rest}
  end

  defp decode_entry!(:chunk_track_snapshot, <<0::1, length::unsigned-integer-size(31), rest::binary>>) do
    <<data::binary-size(^length), rest::binary>> = rest

    stream =
      Stream.repeatedly(fn -> nil end)
      |> Stream.transform(data, fn
        _, <<>> -> {:halt, nil}
        _, data -> ChunkTrackSnapshot.decode_entries!(data)
      end)

    {Enum.to_list(stream), rest}
  end

  defp decode_entry!(:chunk_user, <<1::1, compression::integer-size(3), reserved::integer-size(4), rest::binary>>) do
    <<
      num_records::unsigned-integer-size(16),
      uncompressed_length::unsigned-integer-size(32),
      length::unsigned-integer-size(32),
      body::binary-size(length),
      rest::binary
    >> = rest

    entry = %SubBatchEntry{
      compression: compression,
      reserved: reserved,
      num_records: num_records,
      uncompressed_length: uncompressed_length,
      length: length,
      body: body
    }

    {entry, rest}
  end

  @doc false
  def decode!(<<@magic::unsigned-integer-size(4), @version::unsigned-integer-size(4), rest::binary>>) do
    <<
      chunk_type::integer-size(8),
      num_entries::unsigned-integer-size(16),
      num_records::unsigned-integer-size(32),
      timestamp::integer-size(64),
      epoch::unsigned-integer-size(64),
      chunk_id::unsigned-integer-size(64),
      chunk_crc::integer-size(32),
      data_length::unsigned-integer-size(32),
      trailer_length::unsigned-integer-size(32),
      _reserved::unsigned-integer-size(32),
      data::binary-size(data_length)
    >> = rest

    chunk_type =
      case chunk_type do
        0x00 ->
          :chunk_user

        0x01 ->
          :chunk_track_delta

        0x02 ->
          :chunk_track_snapshot

        _ ->
          raise "Unknown chunk type: #{chunk_type}"
      end

    {data_entries, _rest} =
      Stream.duplicate(chunk_type, num_entries)
      |> Enum.map_reduce(data, &decode_entry!/2)

    %__MODULE__{
      chunk_type: chunk_type,
      num_entries: num_entries,
      num_records: num_records,
      timestamp: timestamp,
      epoch: epoch,
      chunk_id: chunk_id,
      chunk_crc: chunk_crc,
      data_length: data_length,
      trailer_length: trailer_length,
      data_entries: data_entries
    }
  end
end
