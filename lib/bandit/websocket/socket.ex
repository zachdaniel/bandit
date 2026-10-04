defprotocol Bandit.WebSocket.Socket do
  @moduledoc false
  #
  # A protocol defining the low-level functionality of a WebSocket
  #

  @type t :: term()
  @type frame_type :: :text | :binary | :ping | :pong
  @type send_frame_stats :: [
          send_binary_frame_bytes: non_neg_integer(),
          send_binary_frame_count: non_neg_integer(),
          send_ping_frame_bytes: non_neg_integer(),
          send_ping_frame_count: non_neg_integer(),
          send_pong_frame_bytes: non_neg_integer(),
          send_pong_frame_count: non_neg_integer(),
          send_text_frame_bytes: non_neg_integer(),
          send_text_frame_count: non_neg_integer()
        ]

  @spec send_frame(t(), {frame_type :: frame_type(), data :: iodata()}, boolean()) ::
          send_frame_stats()
  def send_frame(socket, data_and_frame_type, compressed)

  @doc """
  Sends uncompressed text or binary data frames in a single socket write, so a
  connection with several queued frames costs one system call rather than one
  per frame.
  """
  @spec send_frames(t(), [{:text | :binary, iodata()}]) :: send_frame_stats()
  def send_frames(socket, data_and_frame_types)

  @spec close(t(), code :: WebSock.close_detail()) :: :ok | {:error, :inet.posix()}
  def close(socket, code)
end

defimpl Bandit.WebSocket.Socket, for: ThousandIsland.Socket do
  @moduledoc false
  #
  # An implementation of Bandit.WebSocket.Socket for use with ThousandIsland.Socket instances
  #

  alias Bandit.WebSocket.Frame

  @spec send_frame(@for.t(), {frame_type :: @protocol.frame_type(), data :: iodata()}, boolean()) ::
          @protocol.send_frame_stats()
  def send_frame(socket, {:text, data}, compressed) do
    do_send_frame(socket, %Frame.Text{fin: true, data: data, compressed: compressed})
    [send_text_frame_count: 1, send_text_frame_bytes: IO.iodata_length(data)]
  end

  def send_frame(socket, {:binary, data}, compressed) do
    do_send_frame(socket, %Frame.Binary{fin: true, data: data, compressed: compressed})
    [send_binary_frame_count: 1, send_binary_frame_bytes: IO.iodata_length(data)]
  end

  def send_frame(socket, {:ping, data}, false) do
    do_send_frame(socket, %Frame.Ping{data: data})
    [send_ping_frame_count: 1, send_ping_frame_bytes: IO.iodata_length(data)]
  end

  def send_frame(socket, {:pong, data}, false) do
    do_send_frame(socket, %Frame.Pong{data: data})
    [send_pong_frame_count: 1, send_pong_frame_bytes: IO.iodata_length(data)]
  end

  @spec send_frames(@for.t(), [{:text | :binary, iodata()}]) :: @protocol.send_frame_stats()
  def send_frames(socket, frames) do
    {data, stats} =
      Enum.map_reduce(frames, [], fn
        {:text, data}, stats ->
          {Frame.serialize(%Frame.Text{fin: true, data: data}),
           add_stats(stats, :send_text_frame_count, :send_text_frame_bytes, data)}

        {:binary, data}, stats ->
          {Frame.serialize(%Frame.Binary{fin: true, data: data}),
           add_stats(stats, :send_binary_frame_count, :send_binary_frame_bytes, data)}
      end)

    @for.send(socket, data)
    stats
  end

  defp add_stats(stats, count, bytes, data) do
    stats
    |> Keyword.update(count, 1, &(&1 + 1))
    |> Keyword.update(bytes, IO.iodata_length(data), &(&1 + IO.iodata_length(data)))
  end

  @spec close(@for.t(), non_neg_integer() | {non_neg_integer(), binary()}) ::
          :ok | {:error, :inet.posix()}
  def close(socket, {code, detail}) when is_integer(code) do
    do_send_frame(socket, %Frame.ConnectionClose{code: code, reason: detail})
    @for.shutdown(socket, :write)
  end

  def close(socket, code) when is_integer(code) do
    do_send_frame(socket, %Frame.ConnectionClose{code: code})
    @for.shutdown(socket, :write)
  end

  @spec do_send_frame(@for.t(), Frame.frame()) ::
          :ok | {:error, :closed | :timeout | :inet.posix()}
  defp do_send_frame(socket, frame) do
    @for.send(socket, Frame.serialize(frame))
  end
end
