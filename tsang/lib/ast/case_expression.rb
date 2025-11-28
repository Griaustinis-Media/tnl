require_relative 'node'

module Tsang
  module AST
    class CaseExpression < Node
      attr_reader :expression, :when_clauses, :else_clause
      
      def initialize(expression: nil, when_clauses:, else_clause: nil)
        @expression = expression
        @when_clauses = when_clauses
        @else_clause = else_clause
      end
    end
  end
end
