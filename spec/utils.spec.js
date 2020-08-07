import { expect } from 'chai';
import { versionCompare } from '../src/utils';

describe('utils', () => {
  describe('.versionCompare', () => {
    [
      ['8.0.2', '8.0.1', 1],
      ['8.0.2', '8.0.3', -1],
      ['8.0.2.', '8.1', -1],
      ['8.0.2', '8', 0],
      ['8.0', '8', 0],
      ['8', '8', 0],
      ['8', '8.0.2', 0],
      ['8', '8.0', 0],
      ['8.0.2', '12.3', -1],
      ['12.3', '8', 1],
      ['12', '8', 1],
      ['8', '12', -1],
    ].forEach(([versionA, versionB, expected]) => {
      it(`.versionCompare('${versionA}', '${versionB}') === ${expected}`, () => {
        expect(versionCompare(versionA, versionB)).to.be.eql(expected);
      });
    });
  });
});
