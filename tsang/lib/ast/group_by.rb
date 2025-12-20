require_relative 'node'

module Tsang
  module AST
    class GroupBy < Node
      attr_reader :expressions

      def initialize(expressions:)
        @expressions = expressions
      end
    end
  end
end
