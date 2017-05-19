import { expect } from 'chai';
import { config } from '../src';
import { readJSONFile } from './../src/utils';
import utilsStub from './utils-stub';


describe('config', () => {
  utilsStub.getConfigPath.install({ copyFixtureToTemp: true });

  describe('.prepare', () => {
    it('should include id for those servers without it', async () => {
      const findItem = (data) => data.servers.find((srv) => srv.name === 'without-id');

      const fixtureBefore = await loadConfig();
      await config.prepare();
      const fixtureAfter = await loadConfig();

      expect(findItem(fixtureBefore)).to.not.have.property('id');
      expect(findItem(fixtureAfter)).to.have.property('id');
    });

    it('should include hashedPwd = true for all servers', async () => {
      await config.prepare();
      const fixtureAfter = await loadConfig();

      const hashedPwdCount =
        fixtureAfter.servers.reduce((previous, curr) => previous + (curr.hashedPwd ? 1 : 0), 0);

      expect(hashedPwdCount).to.equal(fixtureAfter.servers.length);
    });
  });

  function loadConfig() {
    return readJSONFile(utilsStub.TMP_FIXTURE_PATH);
  }
});
