require_relative 'node'

module Tsang
  module AST
    class BinaryOp < Node
      attr_reader :left, :operator, :right
      
      def initialize(left:, operator:, right:)
        @left = left
        @operator = operator
        @right = right
      end
    end
  end
end
