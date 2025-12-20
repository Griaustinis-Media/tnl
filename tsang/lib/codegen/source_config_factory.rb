module Tsang
  module Codegen
    # Factory for generating source-specific configuration metadata
    # This centralizes all source type logic and makes the template universal
    class SourceConfigFactory
      RESERVED_COLUMNS = [:__row_number].freeze

      # Source type categories
      FILE_SOURCES = [:csv].freeze
      DATABASE_SOURCES = [:cassandra, :postgres, :mysql, :mongodb].freeze
      API_SOURCES = [:druid, :elasticsearch].freeze

      def self.build_config(source_type, table:, schema: nil)
        source_type = source_type.to_sym
        
        {
          type: source_type,
          table: table,
          schema: schema,
          category: categorize_source(source_type),
          connection: connection_config(source_type),
          watermark: watermark_config(source_type),
          reserved_columns: RESERVED_COLUMNS
        }
      end

      private

      def self.categorize_source(type)
        return :file if FILE_SOURCES.include?(type)
        return :database if DATABASE_SOURCES.include?(type)
        return :api if API_SOURCES.include?(type)
        :database # default
      end

      def self.connection_config(type)
        case type
        when :csv
          file_connection_config(type)
        when :druid, :elasticsearch
          api_connection_config(type)
        else
          database_connection_config(type)
        end
      end

      def self.file_connection_config(type)
        {
          env_vars: {
            file_path: "#{type.upcase}_FILE",
            delimiter: "#{type.upcase}_DELIMITER"
          },
          defaults: {
            file_path: default_file_path(type),
            delimiter: ","
          },
          template: :file
        }
      end

      def self.database_connection_config(type)
        type_upper = type.to_s.upcase
        
        {
          env_vars: {
            host: "#{type_upper}_HOST",
            port: "#{type_upper}_PORT",
            username: "#{type_upper}_USER",
            password: "#{type_upper}_PASSWORD",
            keyspace: "#{type_upper}_KEYSPACE",
            database: "#{type_upper}_DB"
          },
          defaults: {
            host: default_host(type),
            port: default_port(type),
            keyspace: nil,
            database: nil
          },
          template: :database
        }
      end

      def self.api_connection_config(type)
        type_upper = type.to_s.upcase
        
        {
          env_vars: {
            url: "#{type_upper}_URL",
            username: "#{type_upper}_USER",
            password: "#{type_upper}_PASSWORD"
          },
          defaults: {
            url: default_url(type)
          },
          template: :api
        }
      end

      def self.watermark_config(type)
        case type
        when :csv
          {
            type: :row_number,
            uses_composite: false,
            timestamp_based: false
          }
        else
          {
            type: :composite,
            uses_composite: true,
            timestamp_based: true
          }
        end
      end

      def self.default_file_path(type)
        case type
        when :csv then './data'
        else './data'
        end
      end

      def self.default_host(type)
        case type
        when :cassandra then '127.0.0.1'
        when :postgres, :postgresql then 'localhost'
        when :mysql then 'localhost'
        when :mongodb then 'localhost'
        else 'localhost'
        end
      end

      def self.default_port(type)
        case type
        when :cassandra then 9042
        when :postgres, :postgresql then 5432
        when :mysql then 3306
        when :mongodb then 27017
        else 8080
        end
      end

      def self.default_url(type)
        case type
        when :druid then 'http://localhost:8888'
        when :elasticsearch then 'http://localhost:9200'
        else 'http://localhost:8080'
        end
      end

      # Helper to generate Clojure config map string for a given source
      def self.generate_clojure_config(source_config)
        config = source_config[:connection]
        case config[:template]
        when :file
          generate_file_config_clojure(source_config)
        when :database
          generate_database_config_clojure(source_config)
        when :api
          generate_api_config_clojure(source_config)
        else
          generate_database_config_clojure(source_config)
        end
      end

      def self.generate_file_config_clojure(source_config)
        type = source_config[:type]
        table = source_config[:table]
        env_vars = source_config[:connection][:env_vars]
        defaults = source_config[:connection][:defaults]

        parts = []
        parts << ":type :#{type}"
        
        # File path
        file_path_expr = if defaults[:file_path]
          "(or (System/getenv \"#{env_vars[:file_path]}\") \"#{defaults[:file_path]}/#{table}.csv\")"
        else
          "(System/getenv \"#{env_vars[:file_path]}\")"
        end
        parts << ":file-path #{file_path_expr}"
        
        # Delimiter
        if env_vars[:delimiter]
          parts << ":delimiter (or (System/getenv \"#{env_vars[:delimiter]}\") \"#{defaults[:delimiter]}\")"
        else
          parts << ":delimiter \"#{defaults[:delimiter]}\""
        end

        "{#{parts.join("\n                   ")}}"
      end

      def self.generate_database_config_clojure(source_config)
        type = source_config[:type]
        env_vars = source_config[:connection][:env_vars]
        defaults = source_config[:connection][:defaults]
        schema = source_config[:schema]

        parts = []
        parts << ":type :#{type}"
        
        # Host
        parts << ":contact-points [(or (System/getenv \"#{env_vars[:host]}\") \"#{defaults[:host]}\")]"
        
        # Port
        parts << ":port (or (some-> (System/getenv \"#{env_vars[:port]}\") Integer/parseInt) #{defaults[:port]})"
        
        # Username
        parts << ":username (System/getenv \"#{env_vars[:username]}\")"
        
        # Password
        parts << ":password (System/getenv \"#{env_vars[:password]}\")"
        
        # Keyspace/Database (for Cassandra, MongoDB, etc.)
        if schema
          parts << ":keyspace \"#{schema}\""
        elsif env_vars[:keyspace]
          parts << ":keyspace (System/getenv \"#{env_vars[:keyspace]}\")"
        end

        "{#{parts.join("\n                   ")}}"
      end

      def self.generate_api_config_clojure(source_config)
        type = source_config[:type]
        env_vars = source_config[:connection][:env_vars]
        defaults = source_config[:connection][:defaults]

        parts = []
        parts << ":type :#{type}"
        
        # URL
        url_expr = if defaults[:url]
          "(or (System/getenv \"#{env_vars[:url]}\") \"#{defaults[:url]}\")"
        else
          "(System/getenv \"#{env_vars[:url]}\")"
        end
        parts << ":base-url #{url_expr}"
        
        # Auth
        if env_vars[:username]
          parts << ":username (System/getenv \"#{env_vars[:username]}\")"
          parts << ":password (System/getenv \"#{env_vars[:password]}\")"
        end

        "{#{parts.join("\n                   ")}}"
      end
    end
  end
end
