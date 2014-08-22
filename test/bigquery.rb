# encoding: UTF-8
require 'minitest/autorun'
require 'yaml'
require 'big_query'
require 'pry-byebug'
require 'vcr'

VCR.configure do |c|
  c.cassette_library_dir = 'fixtures/vcr_cassettes'
  c.hook_into :webmock # or :fakeweb
end

class BigQueryTest < MiniTest::Unit::TestCase
  def client
    VCR.use_cassette('big_query_client') do
      BigQuery::Client.new(config)
    end
  end

  def config
    return @config if @config
    config_data ||= File.expand_path(File.dirname(__FILE__) + "/../.bigquery_settings.yml")
    @config = YAML.load_file(config_data)
  end

  def test_for_tables
    VCR.use_cassette('bq_test_for_tables') do
      tables = client.tables

      assert_equal tables[0]['kind'], "bigquery#table"
      assert_includes tables.map { |t| t['id'] }, "#{config['project_id']}:#{config['dataset']}.test"
      assert_includes tables.map { |t| t['tableReference']['tableId'] }, 'test'
    end
  end

  def test_for_tables_formatted
    VCR.use_cassette('bq_test_for_tables_formatted') do
      result = client.tables_formatted

      assert_includes result, 'test'
    end
  end

  def test_for_table_data
    VCR.use_cassette('bq_test_for_table_data') do
      result = client.table_data('test')

      assert_kind_of Array, result
    end
  end

  def test_for_create_table
    VCR.use_cassette('bq_test_for_create_table') do
      if client.tables_formatted.include? 'test123'
        client.delete_table('test123')
      end
      result = client.create_table('test123', id: { type: 'INTEGER' })

      assert_equal result['kind'], "bigquery#table"
      assert_equal result['tableReference']['tableId'], "test123"
      assert_equal result['schema']['fields'], [{"name"=>"id", "type"=>"INTEGER"}]
    end
  end

  def test_for_delete_table
    VCR.use_cassette('bq_test_for_delete_table') do
      if !client.tables_formatted.include? 'test123'
        client.create_table('test123', id: { type: 'INTEGER' })
      end
      result = client.delete_table('test123')

      tables = client.tables_formatted

      refute_includes tables, 'test123'
    end
  end

  def test_for_query
    VCR.use_cassette('bq_test_for_query') do
      result = client.query("SELECT * FROM [#{config['dataset']}.test] LIMIT 1")

      assert_equal result['kind'], "bigquery#queryResponse"
      assert_equal result['jobComplete'], true
    end
  end

  def test_for_insert
    VCR.use_cassette('bq_test_for_insert') do
      result = client.insert('test' ,"id" => 123, "type" => "Task")

      assert_equal result['kind'], "bigquery#tableDataInsertAllResponse"
    end
  end
end
