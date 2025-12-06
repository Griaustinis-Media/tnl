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
          incremental: true
        }
      end

      def generate
        {
          source: extract_source,
          sink: extract_sink,
          columns: extract_columns,
          conditions: extract_conditions,
          timestamp_column: extract_timestamp_column,
          config: config
        }
      end

      private

      def extract_source
        from_node = ast[:from]
        source_type = config[:source_type] || :cassandra
        
        {
          type: source_type,
          table: extract_table_name(from_node),
          schema: extract_schema(from_node),
          default_host: default_host_for(source_type),
          default_port: default_port_for(source_type)
        }
      end

      def extract_sink
        sink_config = config[:sink] || {}
        sink_type = sink_config[:type] || :druid
        
        {
          type: sink_type,
          table: sink_config[:table] || extract_table_name(ast[:from]),
          default_url: sink_config[:default_url] || default_url_for(sink_type)
        }
      end

      def extract_columns
        return [:all] if ast[:columns].nil? || ast[:columns].empty?
        
        ast[:columns].map do |col|
          if col[:type] == :all
            :all
          else
            col[:name]
          end
        end.flatten.uniq
      end

      def extract_conditions
        return [] unless ast[:where]
        parse_conditions(ast[:where])
      end

      def extract_timestamp_column
        config[:timestamp_column] || detect_timestamp_column || :created_at
      end

      def extract_table_name(from_node)
        from_node[:table]
      end

      def extract_schema(from_node)
        from_node[:schema]
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

      def parse_conditions(where_node)
        return [] unless where_node
        
        case where_node[:type]
        when :and
          where_node[:conditions].map { |c| parse_condition(c) }
        when :or
          [{ type: :or, conditions: where_node[:conditions].map { |c| parse_condition(c) } }]
        else
          [parse_condition(where_node)]
        end
      end

      def parse_condition(node)
        {
          column: node[:column],
          operator: node[:operator] || '=',
          value: node[:value]
        }
      end

      def default_host_for(type)
        case type
        when :cassandra then '127.0.0.1'
        when :postgres then 'localhost'
        when :mongodb then 'localhost'
        else 'localhost'
        end
      end

      def default_port_for(type)
        case type.to_sym
        when :cassandra then 9042
        when :postgres then 5432
        when :mongodb then 27017
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
