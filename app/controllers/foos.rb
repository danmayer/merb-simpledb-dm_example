class Foos < Application
  # provides :xml, :yaml, :js

  before :ensure_authenticated

  def index
    #just an example of how to use the ordering and limits on SDB, same as any DM
    @foos = Foo.all(:order => [:time.desc], :limit => 10)
    display @foos
  end

  def show(id)
    @foo = Foo.get(id)
    raise NotFound unless @foo
    display @foo
  end

  def new
    only_provides :html
    @foo = Foo.new
    display @foo
  end

  def edit(id)
    only_provides :html
    @foo = Foo.get(id)
    raise NotFound unless @foo
    display @foo
  end

  def create(foo)
    @foo = Foo.new(foo)
    @foo.id = Time.now.to_i
    if @foo.save
      redirect resource(@foo), :message => {:notice => "Foo was successfully created"}
    else
      message[:error] = "Foo failed to be created"
      render :new
    end
  end

  def update(id, foo)
    @foo = Foo.get(id)
    raise NotFound unless @foo
    if @foo.update_attributes(foo)
       redirect resource(@foo)
    else
      display @foo, :edit
    end
  end

  def destroy(id)
    @foo = Foo.get(id)
    raise NotFound unless @foo
    if @foo.destroy
      redirect resource(:foos)
    else
      raise InternalServerError
    end
  end

end # Foos
