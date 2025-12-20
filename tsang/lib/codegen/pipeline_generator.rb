module Tsang
  module Codegen
    class PipelineGenerator
      attr_reader :ast, :config

      def initialize(ast, config = {})
        @ast = ast
        @config = default_config.merge(config)
      end


      def default_config
        {
          project_name: 'generated-pipeline',
          namespace: 'pipeline',
          batch_size: 5000,
          watermark_enabled: true,
          incremental: true,
          id_column: 'id'
        }
      end

      def generate
        source_config = extract_source
        
        {
          source: source_config,
          source_clojure_config: SourceConfigFactory.generate_clojure_config(source_config),
          sink: extract_sink,
          columns: extract_columns,
          conditions: extract_conditions,
          timestamp_column: extract_timestamp_column,
          id_column: extract_id_column,
          config: config
        }
      end

      private

      def extract_source
        from_node = ast[:from]
        source_type = config[:source_type] || :cassandra
        
        SourceConfigFactory.build_config(
          source_type,
          table: extract_table_name(from_node),
          schema: extract_schema(from_node)
        )
      end

      def extract_sink
        sink_config = config[:sink] || {}
        sink_type = sink_config[:type] || :druid
        
        {
          type: sink_type,
          table: sink_config[:table] || extract_table_name(ast[:from]),
          default_url: sink_config[:default_url] || default_url_for(sink_type),
          config_template: sink_config_template_for(sink_type)
        }
      end

      def extract_columns
        return [:*] if ast[:columns].nil? || ast[:columns].empty?
        
        ast[:columns].map do |col|
          case col[:type]
          when :all
            :*
          when :column
            col[:name] == '*' ? :* : col[:name].to_sym
          when :function
            # For functions, we might want to keep them as-is or format specially
            col[:name].to_sym
          else
            col[:name] ? col[:name].to_sym : :*
          end
        end.flatten.uniq
      end

      def format_value_for_clojure(value)
        return nil unless value
        
        case value[:type]
        when :literal
          # Literals could be strings or numbers, need to check the actual value
          val = value[:value]
          if val.is_a?(Numeric)
            val
          else
            "\"#{val}\""
          end
        when :string
          "\"#{value[:value]}\""
        when :number
          value[:value].to_i
        else
          "\"#{value}\""
        end
      end

      def sink_config_template_for(type)
        type_sym = type.to_sym
        type_upper = type.to_s.upcase
        
        case type_sym
        when :dunwich
          # REST API sink
          %Q[{:base-url (System/getenv "#{type_upper}_URL")
              :api-key (System/getenv "#{type_upper}_API_KEY")}]
        when :csv
          # File-based sink
          %Q[{:output-dir (or (System/getenv "#{type_upper}_OUTPUT_DIR") "./output")
              :delimiter ","
              :append? true}]
        when :druid
          # URL-based sink with auth
          %Q[{:base-url (or (System/getenv "#{type_upper}_URL") "#{default_url_for(type_sym)}")
              :auth {:type :basic
                      :username (System/getenv "#{type_upper}_USER")
                      :password (System/getenv "#{type_upper}_PASSWORD")}}]
        
          when :elasticsearch
            # URL-based sink with simple auth
            %Q[{:base-url (or (System/getenv "#{type_upper}_URL") "#{default_url_for(type_sym)}")
                :username (System/getenv "#{type_upper}_USER")
                :password (System/getenv "#{type_upper}_PASSWORD")}]
        
          when :postgres, :postgresql
            # JDBC-style with dbname
            %Q[{:host (or (System/getenv "#{type_upper}_HOST") "#{default_host_for(type_sym)}")
                :port #{default_port_for(type_sym)}
                :dbname (or (System/getenv "#{type_upper}_DB") "postgres")
                :user (System/getenv "#{type_upper}_USER")
                :password (System/getenv "#{type_upper}_PASSWORD")}]
          
          else
            # Generic host/port with username/password
            %Q[{:host (or (System/getenv "#{type_upper}_HOST") "#{default_host_for(type_sym)}")
                :port #{default_port_for(type_sym)}
                :username (System/getenv "#{type_upper}_USER")
                :password (System/getenv "#{type_upper}_PASSWORD")}]
          end
      end

      def extract_conditions
        return [] unless ast[:where]
        [parse_where_node(ast[:where])].flatten.compact
      end

      def extract_timestamp_column
        config[:timestamp_column] || detect_timestamp_column || :created_at
      end

      def extract_id_column
        config[:id_column] || :id
      end

      def extract_table_name(from_node)
        from_node[:table]
      end

      def extract_schema(from_node)
        from_node[:schema]
      end

      def extract_column_name(expr)
        return nil unless expr
        
        col_name = case expr[:type]
                  when :column, :identifier
                    expr[:name]
                  when :literal, :string, :number
                    expr[:value]
                  else
                    expr[:name] || expr[:value]
                  end
        
        # Convert to symbol if it's a string
        col_name.is_a?(String) ? col_name.to_sym : col_name
      end

      def extract_value(expr)
        case expr[:type]
        when :literal, :string, :number
          expr[:value]
        else
          expr[:value] || expr.to_s
        end
      end

      def detect_timestamp_column
        return nil unless ast[:where]
        
        time_keywords = ['time', 'date', 'created', 'updated', 'timestamp', 'ts']
        find_time_column(ast[:where], time_keywords)
      end

      def find_time_column(node, keywords)
        return nil unless node.is_a?(Hash)
        
        if node[:column]
          col_name = node[:column].to_s.downcase
          return node[:column] if keywords.any? { |kw| col_name.include?(kw) }
        end
        
        node.values.each do |value|
          if value.is_a?(Hash)
            result = find_time_column(value, keywords)
            return result if result
          elsif value.is_a?(Array)
            value.each do |item|
              result = find_time_column(item, keywords) if item.is_a?(Hash)
              return result if result
            end
          end
        end
        
        nil
      end

      def parse_where_node(node)
        return nil unless node
        
        case node[:type]
        when :and
          # Return array of conditions
          node[:left] && node[:right] ? [parse_where_node(node[:left]), parse_where_node(node[:right])] : nil
        when :or
          # For OR, we'd need more complex handling - for now treat as single condition
          parse_single_condition(node)
        when :in_expression
          parse_single_condition(node)
        when :binary_op
          parse_single_condition(node)
        else
          parse_single_condition(node)
        end
      end

      def parse_single_condition(node)
        case node[:type]
        when :in_expression
          {
            type: 'in_expression',
            column: extract_column_name(node[:expression]),
            values: node[:values].map { |v| format_value_for_clojure(v) },
            values_formatted: node[:values].map { |v| format_value_for_clojure(v) }.join(' '),
            negated: node[:negated] || false
          }
        when :binary_op
          {
            type: 'comparison',
            column: extract_column_name(node[:left]),
            operator: node[:operator],
            value: format_value_for_clojure(node[:right])
          }
        else
          nil
        end
      end

      def default_host_for(type)
        case type
        when :cassandra then '127.0.0.1'
        when :postgres then 'localhost'
        when :mongodb then 'localhost'
        when :csv then './data'
        else 'localhost'
        end
      end

      def default_port_for(type)
        case type.to_sym
        when :cassandra then 9042
        when :postgres then 5432
        when :mongodb then 27017
        when :csv then nil
        else 8080
        end
      end

      def default_url_for(type)
        case type
        when :druid then 'http://localhost:8888'
        when :elasticsearch then 'http://localhost:9200'
        else 'http://localhost:8080'
        end
      end
    end
  end
end
