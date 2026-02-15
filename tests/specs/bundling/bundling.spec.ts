import { afterEach, describe, expect, test } from '@jest/globals';
import bundle from '../../../src';

const COMPONENT_SCHEMA_REF_PREFIX = '#/components/schemas/';

function isFileRef(value: unknown): boolean {
  if (typeof value !== 'string') {
    return false;
  }

  const normalizedValue = value.split('#')[0].toLowerCase();
  return (
    normalizedValue.endsWith('.yaml') ||
    normalizedValue.endsWith('.yml') ||
    normalizedValue.endsWith('.json')
  );
}

describe('[integration testing] bundling normalization should', () => {
  const workingDirectory = process.cwd();

  afterEach(() => {
    process.chdir(workingDirectory);
  });

  test('hoist external schemas into components with compact names', async () => {
    const document = await bundle('tests/specs/bundling/hoisting/asyncapi.yaml');
    const asyncapi = document.json() as Record<string, any>;

    expect(asyncapi.components?.schemas).toBeDefined();
    expect(Object.keys(asyncapi.components.schemas)).toEqual(
      expect.arrayContaining(['Pet', 'Owner'])
    );
    expect(asyncapi.channels.pets.messages.petCreated.payload.$ref).toBe(
      '#/components/schemas/Pet'
    );
    expect(asyncapi.components.schemas.Pet.properties.owner.$ref).toBe(
      '#/components/schemas/Owner'
    );
  });

  test('deduplicate equivalent schemas to a canonical compact name', async () => {
    const document = await bundle('tests/specs/bundling/deduplication/asyncapi.yaml');
    const asyncapi = document.json() as Record<string, any>;

    expect(Object.keys(asyncapi.components.schemas)).toContain('Pet');
    expect(Object.keys(asyncapi.components.schemas)).not.toContain('Pet_2');
    expect(asyncapi.channels.petsA.messages.petA.payload.$ref).toBe(
      '#/components/schemas/Pet'
    );
    expect(asyncapi.channels.petsB.messages.petB.payload.$ref).toBe(
      '#/components/schemas/Pet'
    );
  });

  test('rewrite external operation channel refs to local refs', async () => {
    const document = await bundle('tests/specs/bundling/channel-refs/asyncapi.yaml');
    const asyncapi = document.json() as Record<string, any>;

    expect(asyncapi.operations.receivePet.channel.$ref).toBe('#/channels/pets');
  });

  test('rewrite discriminator file mappings to local schema refs', async () => {
    const document = await bundle('tests/specs/bundling/mapping/asyncapi.yaml');
    const asyncapi = document.json() as Record<string, any>;
    const animalSchema = asyncapi.components.schemas.Animal;

    expect(animalSchema.discriminator).toBe('kind');
    expect(animalSchema['x-discriminator-mapping']).toMatchObject({
      pet: '#/components/schemas/Pet',
      owner: '#/components/schemas/Owner',
    });

    const mappingValues = Object.values(
      animalSchema['x-discriminator-mapping'] as Record<string, string>
    );
    expect(
      mappingValues.every(
        value =>
          typeof value === 'string' && value.startsWith(COMPONENT_SCHEMA_REF_PREFIX)
      )
    ).toBeTruthy();
    expect(
      mappingValues.some(value => isFileRef(value))
    ).toBeFalsy();

    const objectDiscriminatorCount = Object.values(asyncapi.components.schemas).filter(
      (schema: any) =>
        schema && typeof schema.discriminator === 'object' && schema.discriminator !== null
    ).length;
    expect(objectDiscriminatorCount).toBe(0);
  });

  test('rewrite refs in schema contexts to component schema refs', async () => {
    const document = await bundle(
      'tests/specs/bundling/schema-context-refs/asyncapi.yaml'
    );
    const asyncapi = document.json() as Record<string, any>;

    expect(asyncapi.channels.schemaChecks.messages.checked.headers.properties.owner.$ref).toBe(
      '#/components/schemas/Owner'
    );
    expect(asyncapi.channels.schemaChecks.messages.checked.payload.properties.pet.$ref).toBe(
      '#/components/schemas/Pet'
    );
    expect(asyncapi.channels.schemaChecks.messages.checked.payload.allOf[0].$ref).toBe(
      '#/components/schemas/Pet'
    );
    expect(
      asyncapi.channels.schemaChecks.messages.checked.payload.allOf[1].properties.owner.$ref
    ).toBe('#/components/schemas/Owner');
    expect(asyncapi.channels.schemaChecks.messages.checked.payload.anyOf[0].$ref).toBe(
      '#/components/schemas/Pet'
    );
    expect(
      asyncapi.channels.schemaChecks.messages.checked.payload.anyOf[1].properties.owners.items
        .$ref
    ).toBe('#/components/schemas/Owner');
    expect(asyncapi.components.schemas.Bag.additionalProperties.$ref).toBe(
      '#/components/schemas/Pet'
    );
    expect(() => JSON.stringify(asyncapi)).not.toThrow();
  });
});
