require File.join(File.dirname(__FILE__), '..', 'spec_helper.rb')

describe "/demo" do
  before(:each) do
    @response = request("/demo")
  end
end