require_relative 'node'

module Tsang
  module AST
    class InExpression < Node
      attr_reader :expression, :values, :negated
      
      def initialize(expression:, values:, negated: false)
        @expression = expression
        @values = values
        @negated = negated
      end
      
      def to_s
        op = negated ? "NOT IN" : "IN"
        "#{expression} #{op} (#{values.map(&:to_s).join(', ')})"
      end
    end
  end
end
