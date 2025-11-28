require_relative 'node'

module Tsang
  module AST
    class ColumnReference < Node
      attr_reader :table, :name, :alias
      
      def initialize(name:, table: nil, alias_name: nil)
        @name = name
        @table = table
        @alias = alias_name
      end
      
      def full_name
        table ? "#{table}.#{name}" : name
      end
    end
  end
end
