Vagrant.configure("2") do |config|
  config.vm.box = 'ubuntu/xenial64'
  config.vm.hostname = 'piinguin'
  config.ssh.forward_agent = true

  config.vm.provider :virtualbox do |vbox|
    vbox.memory = 4096
  end

  config.vm.define 'vagrant' do |machine|

    dynamodb_port = 8000
    machine.vm.network 'forwarded_port', guest: dynamodb_port, host: dynamodb_port

    machine.vm.provision :ansible_local do |ans|
      ans.playbook = 'playbook.yml'
      ans.galaxy_role_file = 'galaxy-roles.yml'
      ans.provisioning_path = '/vagrant/vagrant'
      ans.verbose = true
    end

    machine.vm.provision :docker do |doc|
      doc.run 'dwmkerr/dynamodb', args: "-d -p #{dynamodb_port}:#{dynamodb_port}"
    end

    machine.vm.provision :shell, inline: 'eval echo "\"$(cat /vagrant/vagrant/up.guidance)\""', env: {
      'DYNAMODB_PORT' => dynamodb_port
    }

  end

end
