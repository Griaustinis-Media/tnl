require_relative 'node'

module Tsang
  module AST
    class TableReference < Node
      attr_reader :schema, :name, :alias
      
      def initialize(name:, schema: nil, alias_name: nil)
        @name = name
        @schema = schema
        @alias = alias_name
      end
      
      def full_name
        schema ? "#{schema}.#{name}" : name
      end
    end
  end
end
