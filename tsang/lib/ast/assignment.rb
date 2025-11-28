require_relative 'node'

module Tsang
  module AST
    class Assignment < Node
      attr_reader :column, :value
      
      def initialize(column:, value:)
        @column = column
        @value = value
      end
    end
  end
end
