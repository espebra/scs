def vm config, name, ip
  config.vm.define name.to_sym do |c|
    c.vm.box_url = 'http://repo.i.bitbit.net/vagrant/rl-sl6.box'
    c.vm.network :private_network, ip: ip
    c.vm.box = 'rl-sl6'
    c.vm.hostname = '%s.local' % name
    c.vm.provider :virtualbox do |v|
      v.customize ['modifyvm', :id, '--name', name]
    end
    config.vm.provision :puppet do |puppet|
      puppet.manifests_path = "puppet/manifests"
      puppet.manifest_file  = "init.pp"
    end
  end
end

Vagrant.configure('2') do |config|
  vm config, 'scs1', '10.0.0.2'
#  vm config, 'scs2', '10.0.0.3'
#  vm config, 'scs3', '10.0.0.4'
end

