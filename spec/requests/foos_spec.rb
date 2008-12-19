require File.join(File.dirname(__FILE__), '..', 'spec_helper.rb')

given "a foo exists" do
  Foo.all.destroy!
  request(resource(:foos), :method => "POST", 
    :params => { :foo => { :id => nil }})
end

describe "resource(:foos)" do
  describe "GET" do
    
    before(:each) do
      @response = request(resource(:foos))
    end
    
    it "responds successfully" do
      @response.should be_successful
    end

    it "contains a list of foos" do
      pending
      @response.should have_xpath("//ul")
    end
    
  end
  
  describe "GET", :given => "a foo exists" do
    before(:each) do
      @response = request(resource(:foos))
    end
    
    it "has a list of foos" do
      pending
      @response.should have_xpath("//ul/li")
    end
  end
  
  describe "a successful POST" do
    before(:each) do
      Foo.all.destroy!
      @response = request(resource(:foos), :method => "POST", 
        :params => { :foo => { :id => nil }})
    end
    
    it "redirects to resource(:foos)" do
      @response.should redirect_to(resource(Foo.first), :message => {:notice => "foo was successfully created"})
    end
    
  end
end

describe "resource(@foo)" do 
  describe "a successful DELETE", :given => "a foo exists" do
     before(:each) do
       @response = request(resource(Foo.first), :method => "DELETE")
     end

     it "should redirect to the index action" do
       @response.should redirect_to(resource(:foos))
     end

   end
end

describe "resource(:foos, :new)" do
  before(:each) do
    @response = request(resource(:foos, :new))
  end
  
  it "responds successfully" do
    @response.should be_successful
  end
end

describe "resource(@foo, :edit)", :given => "a foo exists" do
  before(:each) do
    @response = request(resource(Foo.first, :edit))
  end
  
  it "responds successfully" do
    @response.should be_successful
  end
end

describe "resource(@foo)", :given => "a foo exists" do
  
  describe "GET" do
    before(:each) do
      @response = request(resource(Foo.first))
    end
  
    it "responds successfully" do
      @response.should be_successful
    end
  end
  
  describe "PUT" do
    before(:each) do
      @foo = Foo.first
      @response = request(resource(@foo), :method => "PUT", 
        :params => { :foo => {:id => @foo.id} })
    end
  
    it "redirect to the article show action" do
      @response.should redirect_to(resource(@foo))
    end
  end
  
end

