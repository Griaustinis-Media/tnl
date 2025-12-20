require_relative 'node'

module Tsang
  module AST
    class Join < Node
      attr_reader :type, :table, :condition

      def initialize(type:, table:, condition: nil)
        @type = type # :INNER, :LEFT, :RIGHT, :OUTER
        @table = table
        @condition = condition
      end
    end
  end
end
