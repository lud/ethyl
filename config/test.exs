use Mix.Config

config :logger, :console,
  level: :debug,
  format: "[$level] $message $metadata\n",
  metadata: [:error_code, :file]
