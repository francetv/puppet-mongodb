# frozen_string_literal: true

require 'spec_helper'

describe Facter::Util::Fact do
  before do
    Facter.clear
  end

  describe 'mongodb_is_master' do
    context 'mongo not installed' do
      it { expect(Facter.fact(:mongodb_is_master).value).to eq('not_installed') }
    end
    context 'mongo not configured' do
      before do
        allow(Facter::Util::Resolution).to receive(:which).with('mongo').and_return(true)
        allow(Facter::Util::Resolution).to receive(:which).with('mongod').and_return(true)
        ['/etc/mongod.conf', '/etc/mongodb.conf'].each do |file|
          allow(File).to receive(:exist?).with(file).and_return(false)
        end
        allow(File).to receive(:exist?).with('/root/.mongosh.yaml').and_return(false)
      end

      it { expect(Facter.fact(:mongodb_is_master).value).to eq('not_configured') }
    end

    context 'with mongo < 5' do
      before do
        allow(YAML).to receive(:load_file).and_return({})
        allow(Facter::Util::Resolution).to receive(:which).with('mongo').and_return(true)
        allow(Facter::Util::Resolution).to receive(:which).with('mongod').and_return(true)
        allow(File).to receive(:exist?).with('/etc/mongod.conf').and_return(true)
        allow(File).to receive(:exist?).with('/etc/mongodb.conf').and_return(false)
        allow(File).to receive(:exist?).with('/root/.mongosh.yaml').and_return(false)
        allow(File).to receive(:exist?).with('/root/.mongoshrc.js').and_return(false)
        allow(File).to receive(:exist?).with('/root/.mongorc.js').and_return(true)
        allow(Facter::Core::Execution).to receive(:exec).with('mongo --quiet  --eval "load(\'/root/.mongorc.js\'); printjson(db.adminCommand({ ping: 1 }))"').and_return(<<~EOT)
          {
            ok: 1,
            '$clusterTime': {
              clusterTime: Timestamp({ t: 1687185492, i: 1 }),
              signature: {
                hash: Binary(Buffer.from("77085fa8afbaabf9ed42be8bff5f0149c32ced7c", "hex"), 0),
                keyId: Long("7210754072800919553")
              }
            },
            operationTime: Timestamp({ t: 1687185492, i: 1 })
          }
        EOT
        allow(Facter::Core::Execution).to receive(:exec).with('mongo --quiet  --eval "load(\'/root/.mongorc.js\'); db.isMaster().ismaster"').and_return(true)
      end

      it { expect(Facter.fact(:mongodb_is_master).value).to eq(true) }
    end
    context 'with mongo > 5' do
      before do
        allow(YAML).to receive(:load_file).and_return({})
        allow(Facter::Util::Resolution).to receive(:which).with('mongo').and_return(true)
        allow(Facter::Util::Resolution).to receive(:which).with('mongod').and_return(true)
        allow(File).to receive(:exist?).with('/etc/mongod.conf').and_return(true)
        allow(File).to receive(:exist?).with('/etc/mongodb.conf').and_return(false)
        allow(File).to receive(:exist?).with('/root/.mongosh.yaml').and_return(true)
        allow(File).to receive(:exist?).with('/root/.mongoshrc.js').and_return(true)
        allow(File).to receive(:exist?).with('/root/.mongorc.js').and_return(false)
        allow(Facter::Core::Execution).to receive(:exec).with('mongosh --quiet  --eval "load(\'/root/.mongoshrc.js\'); printjson(db.adminCommand({ ping: 1 }))"').and_return(<<~EOT)
          {
            ok: 1,
            '$clusterTime': {
              clusterTime: Timestamp({ t: 1687185492, i: 1 }),
              signature: {
                hash: Binary(Buffer.from("77085fa8afbaabf9ed42be8bff5f0149c32ced7c", "hex"), 0),
                keyId: Long("7210754072800919553")
              }
            },
            operationTime: Timestamp({ t: 1687185492, i: 1 })
          }
        EOT
        allow(Facter::Core::Execution).to receive(:exec).with('mongosh --quiet  --eval "load(\'/root/.mongoshrc.js\'); db.isMaster().ismaster"').and_return(true)
      end

      it { expect(Facter.fact(:mongodb_is_master).value).to eq(true) }
    end
  end
end
