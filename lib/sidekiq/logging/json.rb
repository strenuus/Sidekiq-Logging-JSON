require "sidekiq/logging/json/version"
require "sidekiq/logging/json"
require "json"

module Sidekiq
  module Logging
    module Json
      class Logger < Sidekiq::Logging::Pretty
        DEFAULT_OPTIONS = {
          :prefix_custom_fields_with_at => true
        }

        def initialize(options = DEFAULT_OPTIONS)
          @options = options
          super()
        end

        # Provide a call() method that returns the formatted message.
        def call(severity, time, program_name, message)
          event = {
            '@timestamp' => time.utc.iso8601,
            custom_json_field_name('fields') => {
              :pid => ::Process.pid,
              :tid => "TID-#{Thread.current.object_id.to_s(36)}"
            },
            custom_json_field_name('type') => 'sidekiq',
            custom_json_field_name('severity') => severity
          }

          unless context.nil?
            event[custom_json_field_name('fields')][:context] = context
            event[custom_json_field_name('fields')][:worker] = context.split(" ")[0]
          end

          event[custom_json_field_name('fields')][:program_name] = program_name unless program_name.nil?

          event.merge(process_message(message)).to_json + "\n"
        end

        def process_message(message)
          case message
          when Exception
            {
              custom_json_field_name('status') => 'exception',
              custom_json_field_name('message') => message.message
            }
          when Hash
            if message["retry"]
              {
                custom_json_field_name('status') => 'retry',
                custom_json_field_name('message') => "#{message['class']} failed, retrying with args #{message['args']}."
              }
            else
              {
                custom_json_field_name('status') => 'dead',
                custom_json_field_name('message') => "#{message['class']} failed with args #{message['args']}, not retrying."
              }
            end
          else
            result = message.split(" ")
            status = result[0].match(/^(start|done|fail):?$/) || []

            {
              custom_json_field_name('status') => status[1],                                   # start or done
              custom_json_field_name('run_time') => status[1] && result[1] && result[1].to_f,  # run time in seconds
              custom_json_field_name('message') => message
            }
          end
        end

        private

        def custom_json_field_name(field)
          @options[:prefix_custom_fields_with_at] == true ? "@#{field}" : field
        end
      end
    end
  end
end
