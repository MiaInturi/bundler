import { afterEach, describe, expect, test } from '@jest/globals';
import bundle from '../../../src';

describe('[integration testing] bundling normalization should', () => {
  const workingDirectory = process.cwd();

  afterEach(() => {
    process.chdir(workingDirectory);
  });

  test('hoist external schemas into components with compact names', async () => {
    const document = await bundle('tests/specs/bundling/hoisting/asyncapi.yaml');
    const asyncapi = document.json() as Record<string, any>;
    const schemaNames = Object.keys(asyncapi.components.schemas);

    expect(asyncapi.components?.schemas).toBeDefined();
    expect(schemaNames).toEqual(
      expect.arrayContaining(['Pet', 'Owner'])
    );
    expect(asyncapi.channels.pets.messages.petCreated.payload.$ref).toBe(
      '#/components/schemas/Pet'
    );
    expect(asyncapi.components.schemas.Pet.properties.owner.$ref).toBe(
      '#/components/schemas/Owner'
    );
    expect(schemaNames.indexOf('Owner')).toBeLessThan(schemaNames.indexOf('Pet'));
  });

  test('order schemas topologically and place allOf child right after parent', async () => {
    const document = await bundle('tests/specs/bundling/topological-order/asyncapi.yaml');
    const asyncapi = document.json() as Record<string, any>;
    const schemaNames = Object.keys(asyncapi.components.schemas);

    const parentIndex = schemaNames.indexOf('Parent');
    const childIndex = schemaNames.indexOf('Child');
    const metaIndex = schemaNames.indexOf('Meta');
    const wrapperIndex = schemaNames.indexOf('Wrapper');

    expect(metaIndex).toBeGreaterThanOrEqual(0);
    expect(parentIndex).toBeGreaterThan(metaIndex);
    expect(childIndex).toBe(parentIndex + 1);
    expect(wrapperIndex).toBeGreaterThan(childIndex);
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

  test('name schemas from component refs in fragments', async () => {
    const document = await bundle(
      'tests/specs/bundling/fragment-schema-names/asyncapi.yaml'
    );
    const asyncapi = document.json() as Record<string, any>;

    expect(Object.keys(asyncapi.components.schemas)).toEqual(
      expect.arrayContaining([
        'PerformPhoneCommunicationRequest',
        'CancelPhoneCommunicationRequest',
        'PhoneCommunicationCompletedEvent',
      ])
    );
    expect(Object.keys(asyncapi.components.schemas)).not.toEqual(
      expect.arrayContaining(['schemas', 'schemas_2', 'schemas_3'])
    );

    expect(
      asyncapi.channels.phoneCommunicationInbound.messages.startPhoneCommunicationMessage
        .payload.$ref
    ).toBe('#/components/schemas/PerformPhoneCommunicationRequest');
    expect(
      asyncapi.channels.phoneCommunicationInbound.messages.cancelPhoneCommunicationMessage
        .payload.$ref
    ).toBe('#/components/schemas/CancelPhoneCommunicationRequest');
    expect(
      asyncapi.channels.phoneCommunicationInbound.messages.phoneCallCompletedEvent.payload.$ref
    ).toBe('#/components/schemas/PhoneCommunicationCompletedEvent');
  });

  test('rewrite external operation channel refs to local refs', async () => {
    const document = await bundle('tests/specs/bundling/channel-refs/asyncapi.yaml');
    const asyncapi = document.json() as Record<string, any>;

    expect(asyncapi.operations.receivePet.channel.$ref).toBe('#/channels/pets');
  });

  test('rewrite discriminator file mappings and omit extension in output', async () => {
    const document = await bundle('tests/specs/bundling/mapping/asyncapi.yaml');
    const asyncapi = document.json() as Record<string, any>;
    const animalSchema = asyncapi.components.schemas.Animal;

    expect(animalSchema.discriminator).toBe('kind');
    expect(JSON.stringify(asyncapi)).not.toContain('x-discriminator-mapping');

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
