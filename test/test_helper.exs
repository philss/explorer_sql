defmodule PSQL do
  # This file was copied from Postgrex project:
  # https://github.com/elixir-ecto/postgrex/blob/e6f18d849476deb324eb0567eee841dd21bbd3bb/test/test_helper.exs#L1
  @pg_env %{"PGUSER" => System.get_env("PGUSER") || "postgres"}

  def cmd(args) do
    {output, status} = System.cmd("psql", args, stderr_to_stdout: true, env: @pg_env)

    if status != 0 do
      IO.puts("""
      Command:

      psql #{Enum.join(args, " ")}

      error'd with:

      #{output}

      Please verify the user "postgres" exists and it has permissions to
      create databases and users. If not, you can create a new user with:

      $ createuser postgres -s --no-password
      """)

      System.halt(1)
    end

    output
  end

  def vsn do
    vsn_select = cmd(["-c", "SELECT version();"])
    [_, major, minor] = Regex.run(~r/PostgreSQL (\d+).(\d+)/, vsn_select)
    {String.to_integer(major), String.to_integer(minor)}
  end

  def supports_sockets? do
    unix_socket_dir = System.get_env("PG_SOCKET_DIR") || "/tmp"
    port = System.get_env("PGPORT") || "5432"
    unix_socket_path = Path.join(unix_socket_dir, ".s.PGSQL.#{port}")
    File.exists?(unix_socket_path)
  end

  def supports_ssl? do
    cmd(["-c", "SHOW ssl"]) =~ "on"
  end

  def supports_logical_replication? do
    cmd(["-c", "SHOW wal_level"]) =~ "logical"
  end
end

PSQL.cmd(["-c", "DROP DATABASE IF EXISTS explorer_sql_test;"])

PSQL.cmd([
  "-c",
  "CREATE DATABASE explorer_sql_test TEMPLATE=template0 ENCODING='UTF8' LC_COLLATE='en_US.UTF-8' LC_CTYPE='en_US.UTF-8';"
])

PSQL.cmd([
  "-d",
  "explorer_sql_test",
  "-c",
  "CREATE TABLE links (id serial, url text, clicks int);"
])

PSQL.cmd([
  "-d",
  "explorer_sql_test",
  "-c",
  "INSERT INTO links (url, clicks) VALUES ('https://elixir-lang.org', 42000), ('https://github.com/elixir-nx', 51345), ('https://github.com/elixir-nx/explorer', 63107);"
])

ExUnit.start(assert_receive_timeout: 1_000)
