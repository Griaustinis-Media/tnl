require_relative 'node'

module Tsang
  module AST
    class InsertStatement < Node
      attr_reader :table, :columns, :values
      
      def initialize(table:, columns: [], values:)
        @table = table
        @columns = columns
        @values = values
      end
    end
  end
end
