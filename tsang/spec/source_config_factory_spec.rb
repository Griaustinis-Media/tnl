require 'rspec'
require_relative '../lib/codegen/source_config_factory'

RSpec.describe Tsang::Codegen::SourceConfigFactory do
  describe '.build_config' do
    context 'with CSV source' do
      let(:config) do
        described_class.build_config(:csv, table: 'users', schema: nil)
      end

      it 'categorizes as file source' do
        expect(config[:category]).to eq(:file)
      end

      it 'uses row-based watermark' do
        expect(config[:watermark][:type]).to eq(:row_number)
        expect(config[:watermark][:uses_composite]).to be false
        expect(config[:watermark][:timestamp_based]).to be false
      end

      it 'has file connection template' do
        expect(config[:connection][:template]).to eq(:file)
      end

      it 'sets correct environment variables' do
        expect(config[:connection][:env_vars][:file_path]).to eq('CSV_FILE')
        expect(config[:connection][:env_vars][:delimiter]).to eq('CSV_DELIMITER')
      end
    end

    context 'with Cassandra source' do
      let(:config) do
        described_class.build_config(:cassandra, table: 'events', schema: 'production')
      end

      it 'categorizes as database source' do
        expect(config[:category]).to eq(:database)
      end

      it 'uses composite watermark' do
        expect(config[:watermark][:type]).to eq(:composite)
        expect(config[:watermark][:uses_composite]).to be true
        expect(config[:watermark][:timestamp_based]).to be true
      end

      it 'has database connection template' do
        expect(config[:connection][:template]).to eq(:database)
      end

      it 'sets correct environment variables' do
        expect(config[:connection][:env_vars][:host]).to eq('CASSANDRA_HOST')
        expect(config[:connection][:env_vars][:username]).to eq('CASSANDRA_USER')
        expect(config[:connection][:env_vars][:password]).to eq('CASSANDRA_PASSWORD')
      end

      it 'sets correct defaults' do
        expect(config[:connection][:defaults][:host]).to eq('127.0.0.1')
        expect(config[:connection][:defaults][:port]).to eq(9042)
      end

      it 'includes schema' do
        expect(config[:schema]).to eq('production')
      end
    end

    context 'with Postgres source' do
      let(:config) do
        described_class.build_config(:postgres, table: 'orders', schema: 'analytics')
      end

      it 'categorizes as database source' do
        expect(config[:category]).to eq(:database)
      end

      it 'sets correct defaults' do
        expect(config[:connection][:defaults][:host]).to eq('localhost')
        expect(config[:connection][:defaults][:port]).to eq(5432)
      end

      it 'sets correct environment variables' do
        expect(config[:connection][:env_vars][:host]).to eq('POSTGRES_HOST')
      end
    end

    context 'with Druid source' do
      let(:config) do
        described_class.build_config(:druid, table: 'metrics', schema: nil)
      end

      it 'categorizes as API source' do
        expect(config[:category]).to eq(:api)
      end

      it 'has API connection template' do
        expect(config[:connection][:template]).to eq(:api)
      end

      it 'sets correct environment variables' do
        expect(config[:connection][:env_vars][:url]).to eq('DRUID_URL')
        expect(config[:connection][:env_vars][:username]).to eq('DRUID_USER')
      end

      it 'sets correct URL default' do
        expect(config[:connection][:defaults][:url]).to eq('http://localhost:8888')
      end
    end

    context 'with Elasticsearch source' do
      let(:config) do
        described_class.build_config(:elasticsearch, table: 'logs', schema: nil)
      end

      it 'categorizes as API source' do
        expect(config[:category]).to eq(:api)
      end

      it 'sets correct URL default' do
        expect(config[:connection][:defaults][:url]).to eq('http://localhost:9200')
      end
    end
  end

  describe '.generate_clojure_config' do
    context 'for CSV source' do
      let(:source_config) do
        described_class.build_config(:csv, table: 'users', schema: nil)
      end

      let(:clojure_config) do
        described_class.generate_clojure_config(source_config)
      end

      it 'generates file-based config' do
        expect(clojure_config).to include(':type :csv')
        expect(clojure_config).to include(':file-path')
        expect(clojure_config).to include('CSV_FILE')
        expect(clojure_config).to include(':delimiter')
      end

      it 'includes table in file path' do
        expect(clojure_config).to include('users.csv')
      end
    end

    context 'for Cassandra source' do
      let(:source_config) do
        described_class.build_config(:cassandra, table: 'events', schema: 'production')
      end

      let(:clojure_config) do
        described_class.generate_clojure_config(source_config)
      end

      it 'generates database config' do
        expect(clojure_config).to include(':type :cassandra')
        expect(clojure_config).to include(':contact-points')
        expect(clojure_config).to include('CASSANDRA_HOST')
        expect(clojure_config).to include(':port')
        expect(clojure_config).to include('9042')
        expect(clojure_config).to include(':username')
        expect(clojure_config).to include(':password')
      end

      it 'includes keyspace' do
        expect(clojure_config).to include(':keyspace "production"')
      end
    end

    context 'for Druid source' do
      let(:source_config) do
        described_class.build_config(:druid, table: 'metrics', schema: nil)
      end

      let(:clojure_config) do
        described_class.generate_clojure_config(source_config)
      end

      it 'generates API config' do
        expect(clojure_config).to include(':type :druid')
        expect(clojure_config).to include(':base-url')
        expect(clojure_config).to include('DRUID_URL')
        expect(clojure_config).to include('http://localhost:8888')
        expect(clojure_config).to include(':username')
        expect(clojure_config).to include(':password')
      end
    end

    context 'for Postgres source without schema' do
      let(:source_config) do
        described_class.build_config(:postgres, table: 'transactions', schema: nil)
      end

      let(:clojure_config) do
        described_class.generate_clojure_config(source_config)
      end

      it 'includes keyspace env var reference' do
        expect(clojure_config).to include('POSTGRES_KEYSPACE')
      end
    end
  end

  describe 'reserved columns' do
    it 'defines reserved columns' do
      expect(described_class::RESERVED_COLUMNS).to include(:__row_number)
    end
  end
end
