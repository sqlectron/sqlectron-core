import { expect } from 'chai';
import { ADAPTERS, CLIENTS, db } from '../src';

describe('sqlectron-db-core exports', () => {
  it('should export ADAPTERS and CLIENTS', () => {
    expect(CLIENTS).to.eql(ADAPTERS);
  });

  it('should export db object', () => {
    expect(db).to.be.a('object');
  });
});
