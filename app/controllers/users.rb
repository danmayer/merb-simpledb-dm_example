class Users < Application
  before :ensure_authenticated, :exclude => [:new, :create]

  def index
    @users = User.all
    render
  end

  def show(id)
    only_provides :html
    @user = User.get(id)
    raise NotFound unless @user
    display @user
  end
  
  def edit(id)
    only_provides :html
    @user = User.get(id)
    raise NotFound unless @user
    display @user
  end

  def new
    only_provides :html
    @user = User.new
    display @user
  end

  def update(id, user)
    @user = User.get(id)
    raise NotFound unless @user
    if @user.update_attributes(user)
      redirect resource(@user)
    else
      display @user, :edit
    end
  end


  def create(user)
    session.abandon!
    @user = User.new(user)
    @user.id = Time.now.to_i
    if @user.save
      redirect "/", :message => {:notice => "Signup complete"}
    else
      message[:error] = "Signup failed"
      render :new
    end
  end

end
