require_relative 'node'

module Tsang
  module AST
    class OrderBy < Node
      attr_reader :expression, :direction

      def initialize(expression:, direction: :ASC)
        @expression = expression
        @direction = direction
      end
    end
  end
end
