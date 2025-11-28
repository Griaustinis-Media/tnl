require_relative 'node'

module Tsang
  module AST
    class FunctionCall < Node
      attr_reader :name, :arguments, :distinct
      
      def initialize(name:, arguments: [], distinct: false)
        @name = name
        @arguments = arguments
        @distinct = distinct
      end
    end
  end
end
