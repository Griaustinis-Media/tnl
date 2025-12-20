require_relative '../lib/tsang'

RSpec.describe Tsang do
  describe '.tokenize' do
    it 'tokenizes a simple SELECT statement' do
      sql = 'SELECT id, name FROM users'
      tokens = Tsang.tokenize(sql)

      expect(tokens.map(&:type)).to eq(%i[
                                         SELECT IDENTIFIER COMMA IDENTIFIER FROM IDENTIFIER EOF
                                       ])
    end

    it 'tokenizes numbers' do
      sql = 'SELECT * FROM users WHERE age > 18'
      tokens = Tsang.tokenize(sql)

      number_token = tokens.find { |t| t.type == :NUMBER }
      expect(number_token.value).to eq('18')
    end

    it 'tokenizes string literals' do
      sql = "SELECT * FROM users WHERE name = 'John Doe'"
      tokens = Tsang.tokenize(sql)

      string_token = tokens.find { |t| t.type == :STRING }
      expect(string_token.value).to eq('John Doe')
    end
  end

  describe '.parse' do
    describe 'SELECT statements' do
      it 'parses a simple SELECT' do
        sql = 'SELECT id, name FROM users'
        ast = Tsang.parse(sql)

        expect(ast).to be_a(Tsang::AST::SelectStatement)
        expect(ast.columns.length).to eq(2)
        expect(ast.from.name).to eq('users')
      end

      it 'parses SELECT with WHERE clause' do
        sql = 'SELECT * FROM users WHERE age > 18'
        ast = Tsang.parse(sql)

        expect(ast.where).to be_a(Tsang::AST::BinaryOp)
        expect(ast.where.operator).to eq('>')
      end

      it 'parses SELECT with JOIN' do
        sql = 'SELECT u.name, o.total FROM users u JOIN orders o ON u.id = o.user_id'
        ast = Tsang.parse(sql)

        expect(ast.joins.length).to eq(1)
        expect(ast.joins.first.type).to eq(:INNER)
        expect(ast.joins.first.table.name).to eq('orders')
      end

      it 'parses SELECT with GROUP BY' do
        sql = 'SELECT department, COUNT(*) FROM employees GROUP BY department'
        ast = Tsang.parse(sql)

        expect(ast.group_by).to be_a(Tsang::AST::GroupBy)
        expect(ast.group_by.expressions.length).to eq(1)
      end

      it 'parses SELECT with ORDER BY' do
        sql = 'SELECT * FROM users ORDER BY name ASC, age DESC'
        ast = Tsang.parse(sql)

        expect(ast.order_by.length).to eq(2)
        expect(ast.order_by.first.direction).to eq(:ASC)
        expect(ast.order_by.last.direction).to eq(:DESC)
      end

      it 'parses SELECT with LIMIT and OFFSET' do
        sql = 'SELECT * FROM users LIMIT 10 OFFSET 20'
        ast = Tsang.parse(sql)

        expect(ast.limit).to eq(10)
        expect(ast.offset).to eq(20)
      end

      it 'parses aggregate functions' do
        sql = 'SELECT COUNT(*), SUM(salary), AVG(age) FROM employees'
        ast = Tsang.parse(sql)

        expect(ast.columns.length).to eq(3)
        expect(ast.columns.all? { |c| c.is_a?(Tsang::AST::FunctionCall) }).to be true
      end

      it 'parses table aliases' do
        sql = 'SELECT u.name FROM users AS u'
        ast = Tsang.parse(sql)

        expect(ast.from.alias).to eq('u')
      end
    end

    describe 'INSERT statements' do
      it 'parses INSERT with column list' do
        sql = "INSERT INTO users (name, email) VALUES ('John', 'john@example.com')"
        ast = Tsang.parse(sql)

        expect(ast).to be_a(Tsang::AST::InsertStatement)
        expect(ast.table.name).to eq('users')
        expect(ast.columns).to eq(%w[name email])
        expect(ast.values.length).to eq(1)
      end

      it 'parses INSERT without column list' do
        sql = "INSERT INTO users VALUES ('John', 'john@example.com', 30)"
        ast = Tsang.parse(sql)

        expect(ast.columns).to be_empty
        expect(ast.values.first.length).to eq(3)
      end

      it 'parses multi-row INSERT' do
        sql = "INSERT INTO users (name) VALUES ('John'), ('Jane'), ('Bob')"
        ast = Tsang.parse(sql)

        expect(ast.values.length).to eq(3)
      end
    end

    describe 'UPDATE statements' do
      it 'parses UPDATE with single assignment' do
        sql = "UPDATE users SET email = 'new@example.com' WHERE id = 1"
        ast = Tsang.parse(sql)

        expect(ast).to be_a(Tsang::AST::UpdateStatement)
        expect(ast.assignments.length).to eq(1)
        expect(ast.where).not_to be_nil
      end

      it 'parses UPDATE with multiple assignments' do
        sql = "UPDATE users SET email = 'new@example.com', age = 30 WHERE id = 1"
        ast = Tsang.parse(sql)

        expect(ast.assignments.length).to eq(2)
      end
    end

    describe 'DELETE statements' do
      it 'parses DELETE with WHERE' do
        sql = 'DELETE FROM users WHERE age < 18'
        ast = Tsang.parse(sql)

        expect(ast).to be_a(Tsang::AST::DeleteStatement)
        expect(ast.table.name).to eq('users')
        expect(ast.where).not_to be_nil
      end

      it 'parses DELETE without WHERE' do
        sql = 'DELETE FROM users'
        ast = Tsang.parse(sql)

        expect(ast.where).to be_nil
      end
    end

    describe 'expressions' do
      it 'parses AND/OR expressions' do
        sql = 'SELECT * FROM users WHERE age > 18 AND status = "active" OR role = "admin"'
        ast = Tsang.parse(sql)

        expect(ast.where).to be_a(Tsang::AST::BinaryOp)
        expect(ast.where.operator).to eq('OR')
      end

      it 'parses arithmetic expressions' do
        sql = 'SELECT price * quantity + tax FROM orders'
        ast = Tsang.parse(sql)

        expect(ast.columns.first).to be_a(Tsang::AST::BinaryOp)
      end

      it 'parses NOT expression' do
        sql = 'SELECT * FROM users WHERE NOT deleted'
        ast = Tsang.parse(sql)

        expect(ast.where).to be_a(Tsang::AST::UnaryOp)
        expect(ast.where.operator).to eq('NOT')
      end
    end

    describe 'IN expressions' do
      it 'parses IN with values' do
        sql = "SELECT * FROM users WHERE status IN ('active', 'pending')"
        ast = Tsang.parse(sql)

        expect(ast.where).to be_a(Tsang::AST::InExpression)
        expect(ast.where.negated).to be false
        expect(ast.where.values.length).to eq(2)
      end

      it 'parses NOT IN with values' do
        sql = "SELECT * FROM users WHERE status NOT IN ('deleted', 'banned')"
        ast = Tsang.parse(sql)

        expect(ast.where).to be_a(Tsang::AST::InExpression)
        expect(ast.where.negated).to be true
        expect(ast.where.values.length).to eq(2)
      end

      it 'parses IN with numbers' do
        sql = 'SELECT * FROM orders WHERE id IN (1, 2, 3, 4, 5)'
        ast = Tsang.parse(sql)

        expect(ast.where).to be_a(Tsang::AST::InExpression)
        expect(ast.where.values.length).to eq(5)
      end
    end
  end
end
