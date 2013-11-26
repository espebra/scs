$ngx_version = '1.4.2.9'

# Install some packages 
$packages = [ 'git', 'rsync', 'python-devel', 'openssl-devel', 'pcre-devel',
              'gcc', 'xinetd', 'perl-libwww-perl', 'lsof', 'mlocate',
              'strace' ]

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
        owner   => 'scs',
        group   => 'scs',
        require => [File['/srv'],User['scs']];
    '/srv/files/queue':
        ensure  => directory,
        owner   => 'scs',
        group   => 'scs',
        require => [File['/srv/files'],User['scs']];
    '/srv/files/objects':
        ensure  => directory,
        owner   => 'scs',
        group   => 'scs',
        require => [File['/srv/files'],User['scs']];
    '/etc/scs':
        ensure  => directory;
    '/srv/scs':
        ensure => link,
        require => File['/srv'],
        target => '/vagrant/scs';
    '/var/log/scs':
        ensure => directory,
        owner  => 'scs',
        group  => 'scs',
        require => User['scs'];
    '/var/cache/scs':
        ensure => directory,
        owner  => 'scs',
        group  => 'scs',
        require => User['scs'];
}

# Configuration
file {
    '/etc/xinetd.d/rsync':
        ensure  => present,
        source  => '/vagrant/puppet/files/xinetd/rsync',
        notify  => Service['xinetd'];
    '/etc/rsyncd.conf':
        ensure  => present,
        source  => '/vagrant/scs/conf/rsyncd.conf',
        notify  => Service['xinetd'];
    '/etc/init.d/replicator':
        ensure  => link,
        source  => '/vagrant/bin/replicator.init',
        notify  => Service['scs'];
    '/etc/init.d/scs':
        ensure  => link,
        source  => '/vagrant/scs/conf/scs.init',
        notify  => Service['scs'];
    '/etc/scs/hosts.conf':
        ensure  => link,
        source  => '/vagrant/scs/conf/hosts.conf',
        notify  => Service['scs'];
    '/etc/scs/common.conf':
        ensure  => link,
        source  => '/vagrant/scs/conf/common.conf',
        notify  => Service['scs'];
    '/etc/scs/local.conf':
        ensure  => link,
        source  => '/vagrant/scs/conf/local.conf',
        notify  => Service['scs'];
    '/usr/local/sbin/p':
        source  => '/vagrant/puppet/files/puppet/p',
        mode    => 555;
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
        timeout     => 0,
        cwd         => '/vagrant/src',
        creates     => "/tmp/ngx_openresty-${ngx_version}",
        notify      => Exec['configure'];
    'configure':
        command   => 'configure --with-luajit --with-pcre-jit --prefix=/usr/local/openresty --with-ipv6 --error-log-path=/var/log/scs/error.log --http-log-path=/var/log/scs/access.log --http-client-body-temp-path=/var/cache/scs/',
        timeout     => 0,
        path      => [ '/bin/', '/sbin/', '/usr/bin/', '/usr/sbin/', "/tmp/ngx_openresty-${ngx_version}" ],
        cwd       => "/tmp/ngx_openresty-${ngx_version}",
        refreshonly => true,
        notify    => Exec['make'];
    'make':
        command   => 'gmake',
        timeout     => 0,
        cwd       => "/tmp/ngx_openresty-${ngx_version}",
        refreshonly => true,
        notify    => Exec['install'];
    'install':
        command   => 'gmake install',
        timeout     => 0,
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
        require    => [Package[$packages],File['/srv/files'],File['/etc/scs/local.conf']],
        hasrestart => true,
        enable     => true;
    'replicator':
        ensure     => true,
        require    => [Package[$packages],File['/srv/files'],File['/etc/init.d/replicator'],File['/etc/scs/local.conf']],
        hasrestart => true,
        enable     => true;
}

cron {
    'puppet':
        command => '/usr/local/sbin/p',
        user    => 'root',
        minute  => [0, 15, 30, 45];
}

