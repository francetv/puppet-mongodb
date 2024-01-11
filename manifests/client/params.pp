# @api private
class mongodb::client::params inherits mongodb::globals {
  $package_ensure = pick($mongodb::globals::version, 'present')
  $manage_package = pick($mongodb::globals::manage_package, $mongodb::globals::manage_package_repo, false)

  # the new mongosh package is the same for all dsitros.
  $package_name = 'mongodb-mongosh'
}
