# frozen_string_literal: true

Facter.add(:which_mongo) do
  setcode do
    if Facter::Core::Execution.which('mongosh')
      'mongosh'
    elsif Facter::Core::Execution.which('mongo')
      'mongo'
    end
  end
end
