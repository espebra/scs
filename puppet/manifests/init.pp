$ngx_version = '1.4.2.9'

# Install some packages 
$packages = [ 'git', 'rsync', 'python-devel', 'openssl-devel', 'pcre-devel',
              'gcc', 'xinetd' ]

package {
    $packages: ensure => installed;
}

# Unprivileged service user
user {
    'scs':
        ensure  => present,
        require => Group['scs'],
        uid     => 1234,
        gid     => 1234;
}

group {
    'scs':
        ensure => present,
        gid    => 1234;
}

File {
    require => [ 
                   Package[$packages], 
                   User['scs'] 
               ],
}

# Directory structure
file {
    '/srv':
        ensure => directory;
    '/srv/files':
        ensure  => directory,
        require => File["/srv"];
    '/srv/scs':
        ensure => link,
        require => File["/srv"],
        target => "/vagrant/scs";
    '/var/log/scs':
        ensure => directory,
        owner  => 'scs',
        group  => 'scs';
}

# Configuration
file {
    '/etc/xinetd.d/rsync':
        ensure  => present,
        source  => '/vagrant/puppet/files/xinetd/rsync',
        notify  => Service["xinetd"];
    '/etc/rsyncd.conf':
        ensure  => present,
        source  => '/vagrant/scs/conf/rsyncd.conf',
        notify  => Service["xinetd"];
    '/etc/init.d/scs':
        ensure  => link,
        source  => '/vagrant/scs/conf/scs.init.example',
        notify  => Service["scs"];
}

# Building openresty
Exec {
    require     => Package[$packages], 
    logoutput   => true,
    path        => [ '/bin/', '/sbin/' , '/usr/bin/', '/usr/sbin/' ],
}

exec {
    'extract':
        command     => "tar -xvzf ngx_openresty-${ngx_version}.tar.gz -C /tmp",
        cwd         => '/vagrant',
        creates     => "/tmp/ngx_openresty-${ngx_version}",
        notify      => Exec['configure'];
    'configure':
        command   => 'configure --with-luajit --with-pcre-jit --prefix=/usr/local/openresty --with-ipv6',
        path      => [ '/bin/', '/sbin/', '/usr/bin/', '/usr/sbin/', "/tmp/ngx_openresty-${ngx_version}" ],
        cwd       => "/tmp/ngx_openresty-${ngx_version}",
        refreshonly => true,
        notify    => Exec['make'];
    'make':
        command   => "gmake",
        cwd       => "/tmp/ngx_openresty-${ngx_version}",
        refreshonly => true,
        notify    => Exec['install'];
    'install':
        command   => "gmake install",
        refreshonly => true,
        cwd       => "/tmp/ngx_openresty-${ngx_version}";
}

# Services
service {
    'xinetd':
        ensure     => true,
        require    => Package[$packages],
        hasrestart => true,
        enable     => true;
    'scs':
        ensure     => true,
        require    => [Package[$packages],File["/srv/files"]],
        hasrestart => true,
        enable     => true;
}
