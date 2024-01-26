# @summary Class for setting cross-class global overrides. See README.md for more details.
#
# @param server_package_name
# @param client_package_name
# @param mongod_service_manage
# @param service_enable
# @param service_ensure
# @param service_name
# @param service_provider
# @param service_status
# @param user
# @param group
# @param ipv6
# @param bind_ip
# @param version
# @param manage_package_repo
# @param manage_package
# @param repo_proxy
# @param proxy_username
# @param proxy_password
# @param repo_location
# @param use_enterprise_repo
# @param pidfilepath
# @param pidfilemode
# @param manage_pidfile
#
class mongodb::globals (
  $server_package_name   = undef,
  $client_package_name   = undef,

  $mongod_service_manage = undef,
  $service_enable        = undef,
  $service_ensure        = undef,
  $service_name          = undef,
  $service_provider      = undef,
  $service_status        = undef,

  $user                  = undef,
  $group                 = undef,
  $ipv6                  = undef,
  $bind_ip               = undef,

  $version               = fact('os.distro.codename') ? { # Debian 10 doesn't provide mongodb 3.6.
    'buster' => '4.4.8',
    default  => undef
  },
  $manage_package_repo   = fact('os.distro.codename') ? { # Debian 10 doesn't provide mongodb packages. So manage it!
    'buster' => true,
    default  => undef
  },
  $manage_package        = undef,
  $repo_proxy            = undef,
  $proxy_username        = undef,
  $proxy_password        = undef,

  $repo_location         = undef,
  $use_enterprise_repo   = undef,

  $pidfilepath           = undef,
  $pidfilemode           = undef,
  $manage_pidfile        = undef,
) {
  if $use_enterprise_repo {
    $edition = 'enterprise'
  } else {
    $edition = 'org'
  }

  # Setup of the repo only makes sense globally, so we are doing it here.
  case $facts['os']['family'] {
    'RedHat', 'Linux', 'Suse': {
      # For RedHat, Linux and Suse family: if manage_package_repo is set at undef that include mongodb::repo
      if $manage_package_repo != false {
        class { 'mongodb::repo':
          ensure              => present,
          version             => pick($version, '3.6'),
          use_enterprise_repo => $use_enterprise_repo,
          repo_location       => $repo_location,
          proxy               => $repo_proxy,
        }
      }
    }
    default: {
      # For other (Debian) family: if manage_package_repo is set at undef that not include mongodb::repo
      if $manage_package_repo {
        if $use_enterprise_repo == true and $version == undef {
          fail('You must set mongodb::globals::version when mongodb::globals::use_enterprise_repo is true')
        }

        class { 'mongodb::repo':
          ensure              => present,
          version             => $version,
          use_enterprise_repo => $use_enterprise_repo,
          repo_location       => $repo_location,
          proxy               => $repo_proxy,
        }
      }
    }
  }
}
