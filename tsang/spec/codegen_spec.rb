require_relative '../lib/tsang'
require_relative '../lib/codegen/pipeline_generator'
require_relative '../lib/ast/serializer'

RSpec.describe 'Code Generation' do
  describe 'PipelineGenerator' do
    it 'generates pipeline data from simple SELECT' do
      sql = 'SELECT * FROM users'
      ast_obj = Tsang.parse(sql)
      ast = Tsang::AST::Serializer.to_hash(ast_obj)

      generator = Tsang::Codegen::PipelineGenerator.new(ast, {})
      data = generator.generate

      expect(data[:source][:table]).to eq('users')
      expect(data[:columns]).to include(:*)
    end

    it 'generates pipeline with WHERE IN condition' do
      sql = "SELECT * FROM events WHERE event_type IN ('click', 'view')"
      ast_obj = Tsang.parse(sql)
      ast = Tsang::AST::Serializer.to_hash(ast_obj)

      generator = Tsang::Codegen::PipelineGenerator.new(ast, {})
      data = generator.generate

      expect(data[:conditions].length).to eq(1)
      expect(data[:conditions][0][:type]).to eq('in_expression')
      expect(data[:conditions][0][:column]).to eq(:event_type)
      expect(data[:conditions][0][:values]).to eq(['"click"', '"view"'])
    end

    it 'generates pipeline with WHERE NOT IN condition' do
      sql = "SELECT * FROM events WHERE event_type NOT IN ('ping', 'exit')"
      ast_obj = Tsang.parse(sql)
      ast = Tsang::AST::Serializer.to_hash(ast_obj)

      generator = Tsang::Codegen::PipelineGenerator.new(ast, {})
      data = generator.generate

      expect(data[:conditions].length).to eq(1)
      expect(data[:conditions][0][:type]).to eq('in_expression')
      expect(data[:conditions][0][:negated]).to be true
    end

    it 'extracts schema and table name' do
      sql = 'SELECT * FROM events.tracking_events'
      ast_obj = Tsang.parse(sql)
      ast = Tsang::AST::Serializer.to_hash(ast_obj)

      generator = Tsang::Codegen::PipelineGenerator.new(ast, {})
      data = generator.generate

      expect(data[:source][:schema]).to eq('events')
      expect(data[:source][:table]).to eq('tracking_events')
    end

    it 'detects timestamp column from WHERE clause' do
      sql = 'SELECT * FROM events WHERE created_at > 123'
      ast_obj = Tsang.parse(sql)
      ast = Tsang::AST::Serializer.to_hash(ast_obj)

      generator = Tsang::Codegen::PipelineGenerator.new(ast, {})
      data = generator.generate

      expect(data[:timestamp_column]).to eq(:created_at)
    end

    it 'formats string values with quotes' do
      sql = "SELECT * FROM users WHERE status = 'active'"
      ast_obj = Tsang.parse(sql)
      ast = Tsang::AST::Serializer.to_hash(ast_obj)

      generator = Tsang::Codegen::PipelineGenerator.new(ast, {})
      data = generator.generate

      expect(data[:conditions][0][:value]).to eq('"active"')
    end

    it 'formats number values without quotes' do
      sql = 'SELECT * FROM users WHERE age = 25'
      ast_obj = Tsang.parse(sql)
      ast = Tsang::AST::Serializer.to_hash(ast_obj)

      generator = Tsang::Codegen::PipelineGenerator.new(ast, {})
      data = generator.generate

      expect(data[:conditions][0][:value]).to eq(25)
    end
  end

  describe 'AST Serializer' do
    it 'serializes IN expression' do
      sql = 'SELECT * FROM users WHERE id IN (1, 2, 3)'
      ast = Tsang.parse(sql)
      hash = Tsang::AST::Serializer.to_hash(ast)

      expect(hash[:where][:type]).to eq(:in_expression)
      expect(hash[:where][:values].length).to eq(3)
      expect(hash[:where][:negated]).to be false
    end

    it 'serializes NOT IN expression' do
      sql = "SELECT * FROM users WHERE status NOT IN ('deleted', 'banned')"
      ast = Tsang.parse(sql)
      hash = Tsang::AST::Serializer.to_hash(ast)

      expect(hash[:where][:type]).to eq(:in_expression)
      expect(hash[:where][:negated]).to be true
    end
  end
end
