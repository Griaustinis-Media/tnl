require_relative 'node'

module Tsang
  module AST
    class WhenClause < Node
      attr_reader :condition, :result
      
      def initialize(condition:, result:)
        @condition = condition
        @result = result
      end
    end
  end
end
