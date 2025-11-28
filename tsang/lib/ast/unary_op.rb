require_relative 'node'

module Tsang
  module AST
    class UnaryOp < Node
      attr_reader :operator, :operand
      
      def initialize(operator:, operand:)
        @operator = operator
        @operand = operand
      end
    end
  end
end
