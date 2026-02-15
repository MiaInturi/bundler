/* eslint-disable security/detect-object-injection, security/detect-non-literal-fs-filename, security/detect-unsafe-regex, sonarjs/cognitive-complexity, @typescript-eslint/no-use-before-define */
import { existsSync, readFileSync, readdirSync } from 'fs';
import path from 'path';
import structuredClone from '@ungap/structured-clone';
import $RefParser from '@apidevtools/json-schema-ref-parser';
import yaml from 'js-yaml';
import { isExternalReference } from './util';

import type { AsyncAPIObject } from './spec-types';

const COMPONENT_SCHEMA_REF_PREFIX = '#/components/schemas/';
const X_ORIGIN_KEY = 'x-origin';
const X_DISCRIMINATOR_MAPPING_KEY = 'x-discriminator-mapping';

const schemaDirectKeywords = new Set([
  'schema',
  'payload',
  'headers',
  'items',
  'additionalItems',
  'contains',
  'additionalProperties',
  'propertyNames',
  'if',
  'then',
  'else',
  'not',
  'unevaluatedItems',
  'unevaluatedProperties',
]);

const schemaArrayKeywords = new Set([
  'allOf',
  'anyOf',
  'oneOf',
  'prefixItems',
]);

const schemaMapKeywords = new Set([
  'properties',
  'patternProperties',
  'definitions',
  '$defs',
  'dependentSchemas',
]);

type SchemaComponentState = {
  objectToName: Map<object, string>;
  nameToSchema: Map<string, Record<string, any>>;
  schemaSignatureToName: Map<string, string>;
  fullOriginToName: Map<string, string>;
  basenameToName: Map<string, string>;
  ambiguousBasenames: Set<string>;
  attemptedSchemaLoads: Set<string>;
  fileSearchCache: Map<string, string[]>;
};

type ChannelRefState = {
  fullPathToRef: Map<string, string>;
  basenameToRef: Map<string, string>;
  ambiguousBasenames: Set<string>;
};

type Segment = string | number;
type SchemaVisitor = (
  node: any,
  parent: any,
  key: Segment | undefined,
  pathSegments: Segment[]
) => boolean | void;

function isObject(value: any): value is Record<string, any> {
  return typeof value === 'object' && value !== null;
}

function isReferenceObject(value: any): value is { $ref: string } {
  return isObject(value) && typeof value.$ref === 'string';
}

function isSchemaObject(value: any): value is Record<string, any> {
  return isObject(value) && !Array.isArray(value);
}

function normalizeRefPath(ref: string): string {
  return String(ref).split('#')[0];
}

function getSchemaNameFromRef(ref: string): string {
  const refPath = normalizeRefPath(ref);
  const fileName = path.basename(refPath);
  const baseName = fileName.replace(/\.[^.]+$/, '');
  const sanitized = baseName
    .replace(/[^A-Za-z0-9_.-]/g, '_')
    .replace(/^[_\-.]+|[_\-.]+$/g, '');
  const name = sanitized || 'Schema';
  return (/^\d/).test(name) ? `Schema_${name}` : name;
}

function getOrigin(value: Record<string, any>): string | undefined {
  const origin = value[X_ORIGIN_KEY];
  return typeof origin === 'string' ? origin : undefined;
}

function createSchemaFingerprint(schema: Record<string, any>): string {
  function serialize(node: any, ancestry = new Set<object>()): string {
    if (!isObject(node)) {
      return JSON.stringify(node);
    }

    if (Array.isArray(node)) {
      return `[${node.map(entry => serialize(entry, ancestry)).join(',')}]`;
    }

    if (ancestry.has(node)) {
      return '{"$cycle":true}';
    }

    ancestry.add(node);

    const entries = Object.entries(node)
      .filter(
        ([key]) =>
          key !== X_ORIGIN_KEY && key !== 'description' && key !== 'summary'
      )
      .sort(([a], [b]) => a.localeCompare(b));

    const serialized = `{${entries
      .map(([key, value]) => `${JSON.stringify(key)}:${serialize(value, ancestry)}`)
      .join(',')}}`;

    ancestry.delete(node);
    return serialized;
  }

  return serialize(schema);
}

function createSchemaSignature(
  normalizedName: string,
  schema: Record<string, any>
): string {
  return `${normalizedName}::${createSchemaFingerprint(schema)}`;
}

function createSchemaRef(
  componentName: string,
  sourceSchema?: Record<string, any>
): Record<string, any> {
  const output: Record<string, any> = {
    $ref: `${COMPONENT_SCHEMA_REF_PREFIX}${componentName}`,
  };

  if (sourceSchema) {
    if (typeof sourceSchema.description === 'string') {
      output.description = sourceSchema.description;
    }
    if (typeof sourceSchema.summary === 'string') {
      output.summary = sourceSchema.summary;
    }
  }

  return output;
}

function ensureUniqueSchemaName(
  suggestedName: string,
  schema: Record<string, any>,
  state: SchemaComponentState
): string {
  let candidate = suggestedName;
  let index = 2;

  while (
    state.nameToSchema.has(candidate) &&
    state.nameToSchema.get(candidate) !== schema
  ) {
    candidate = `${suggestedName}_${index}`;
    index++;
  }

  return candidate;
}

function registerSchema(
  schema: Record<string, any>,
  suggestedName: string,
  state: SchemaComponentState,
  originPath?: string
): string {
  const alreadyRegistered = state.objectToName.get(schema);
  if (alreadyRegistered) {
    return alreadyRegistered;
  }

  if (originPath) {
    const mappedByOrigin = state.fullOriginToName.get(originPath);
    if (mappedByOrigin) {
      state.objectToName.set(schema, mappedByOrigin);
      return mappedByOrigin;
    }
  }

  const safeName = getSchemaNameFromRef(suggestedName);
  const signature = createSchemaSignature(safeName, schema);
  const sameSchemaWithSameName = state.schemaSignatureToName.get(signature);

  if (sameSchemaWithSameName) {
    state.objectToName.set(schema, sameSchemaWithSameName);

    if (originPath) {
      if (!state.fullOriginToName.has(originPath)) {
        state.fullOriginToName.set(originPath, sameSchemaWithSameName);
      }

      const baseName = path.basename(originPath);
      if (baseName) {
        const existing = state.basenameToName.get(baseName);
        if (existing && existing !== sameSchemaWithSameName) {
          state.ambiguousBasenames.add(baseName);
        } else {
          state.basenameToName.set(baseName, sameSchemaWithSameName);
        }
      }
    }

    return sameSchemaWithSameName;
  }

  const uniqueName = ensureUniqueSchemaName(safeName, schema, state);
  state.objectToName.set(schema, uniqueName);
  state.schemaSignatureToName.set(signature, uniqueName);

  if (!state.nameToSchema.has(uniqueName)) {
    state.nameToSchema.set(uniqueName, schema);
  }

  if (originPath) {
    if (!state.fullOriginToName.has(originPath)) {
      state.fullOriginToName.set(originPath, uniqueName);
    }
    const baseName = path.basename(originPath);
    if (baseName) {
      const existing = state.basenameToName.get(baseName);
      if (existing && existing !== uniqueName) {
        state.ambiguousBasenames.add(baseName);
      } else {
        state.basenameToName.set(baseName, uniqueName);
      }
    }
  }

  return uniqueName;
}

function resolveSchemaNameByExternalRef(
  ref: string,
  state: SchemaComponentState
): string | undefined {
  const byExactPath = state.fullOriginToName.get(ref);
  if (byExactPath) {
    return byExactPath;
  }

  const normalized = normalizeRefPath(ref);

  const byPath = state.fullOriginToName.get(normalized);
  if (byPath) {
    return byPath;
  }

  const baseName = path.basename(normalized);
  if (!baseName || state.ambiguousBasenames.has(baseName)) {
    return;
  }

  return state.basenameToName.get(baseName);
}

function looksLikeFileReference(value: string): boolean {
  const normalizedValue = normalizeRefPath(value).toLowerCase();
  return (
    normalizedValue.endsWith('.yaml') ||
    normalizedValue.endsWith('.yml') ||
    normalizedValue.endsWith('.json')
  );
}

function toPosixPath(value: string): string {
  return value.replace(/\\/g, '/');
}

function findFilesByBasename(
  baseName: string,
  state: SchemaComponentState
): string[] {
  const cached = state.fileSearchCache.get(baseName);
  if (cached) {
    return cached;
  }

  const ignoredDirectories = new Set(['.git', 'node_modules', 'lib']);
  const rootDirectory = process.cwd();
  const discoveredFiles: string[] = [];
  const directoriesToVisit: string[] = [rootDirectory];

  while (directoriesToVisit.length > 0) {
    const currentDirectory = directoriesToVisit.pop();
    if (!currentDirectory) {
      continue;
    }

    const directoryEntries = readdirSync(currentDirectory, { withFileTypes: true });

    for (const entry of directoryEntries) {
      const absoluteEntryPath = path.join(currentDirectory, entry.name);

      if (entry.isDirectory()) {
        if (!ignoredDirectories.has(entry.name)) {
          directoriesToVisit.push(absoluteEntryPath);
        }
        continue;
      }

      if (entry.isFile() && entry.name === baseName) {
        discoveredFiles.push(
          toPosixPath(path.relative(rootDirectory, absoluteEntryPath))
        );
      }
    }
  }

  discoveredFiles.sort((left, right) => left.localeCompare(right));
  state.fileSearchCache.set(baseName, discoveredFiles);
  return discoveredFiles;
}

function getResolvedMappingOriginPath(
  mappingRef: string,
  schemaOrigin: string | undefined,
  state: SchemaComponentState
): string | undefined {
  const normalizedMappingRef = toPosixPath(normalizeRefPath(mappingRef));

  const candidates: string[] = [];
  if (schemaOrigin && isExternalReference(schemaOrigin)) {
    const baseDirectory = path.posix.dirname(toPosixPath(normalizeRefPath(schemaOrigin)));
    candidates.push(path.posix.normalize(path.posix.join(baseDirectory, normalizedMappingRef)));
  }

  candidates.push(path.posix.normalize(normalizedMappingRef));
  candidates.push(path.posix.basename(normalizedMappingRef));

  for (const candidate of candidates) {
    if (existsSync(path.resolve(process.cwd(), candidate))) {
      return candidate;
    }
  }

  const baseName = path.posix.basename(normalizedMappingRef);
  const discoveredCandidates = findFilesByBasename(baseName, state);
  if (discoveredCandidates.length === 1) {
    return discoveredCandidates[0];
  }

  if (discoveredCandidates.length > 1 && schemaOrigin) {
    const schemaDirectoryName = path.posix.basename(
      path.posix.dirname(toPosixPath(normalizeRefPath(schemaOrigin)))
    );
    const sameDirectoryCandidates = discoveredCandidates.filter(candidate =>
      candidate.includes(`/${schemaDirectoryName}/`)
    );

    if (sameDirectoryCandidates.length === 1) {
      return sameDirectoryCandidates[0];
    }
  }
}

function shouldTreatAsSchemaEntryPoint(
  pathSegments: Segment[],
  key: string
): boolean {
  return (
    key === 'schema' ||
    (key === 'schemas' &&
      pathSegments.length === 1 &&
      pathSegments[0] === 'components') ||
    ((key === 'payload' || key === 'headers') &&
      !pathSegments.includes('examples'))
  );
}

function isComponentSchemaPath(pathSegments: Segment[]): boolean {
  return (
    pathSegments.length === 3 &&
    pathSegments[0] === 'components' &&
    pathSegments[1] === 'schemas' &&
    typeof pathSegments[2] === 'string'
  );
}

function walkDocumentSchemaContexts(
  node: any,
  pathSegments: Segment[],
  visitor: SchemaVisitor
): void {
  if (!isObject(node)) {
    return;
  }

  if (Array.isArray(node)) {
    node.forEach((entry, index) => {
      walkDocumentSchemaContexts(entry, [...pathSegments, index], visitor);
    });
    return;
  }

  for (const [key, value] of Object.entries(node)) {
    const nextPath = [...pathSegments, key];

    const isComponentsSchemasEntryPoint =
      key === 'schemas' &&
      pathSegments.length === 1 &&
      pathSegments[0] === 'components' &&
      isSchemaObject(value);

    if (isComponentsSchemasEntryPoint) {
      for (const [schemaName, schema] of Object.entries(value)) {
        walkSchema(schema, value, schemaName, [...nextPath, schemaName], visitor);
      }
      continue;
    }

    if (shouldTreatAsSchemaEntryPoint(pathSegments, key)) {
      walkSchema(value, node, key, nextPath, visitor);
      continue;
    }

    walkDocumentSchemaContexts(value, nextPath, visitor);
  }
}

function walkSchema(
  node: any,
  parent: any,
  key: Segment | undefined,
  pathSegments: Segment[],
  visitor: SchemaVisitor,
  ancestors = new Set<object>()
): void {
  const skipChildren = visitor(node, parent, key, pathSegments) === true;
  if (skipChildren || !isObject(node)) {
    return;
  }

  if (ancestors.has(node)) {
    return;
  }

  ancestors.add(node);

  if (Array.isArray(node)) {
    node.forEach((entry, index) => {
      walkSchema(entry, node, index, [...pathSegments, index], visitor, ancestors);
    });
    ancestors.delete(node);
    return;
  }

  for (const [childKey, childValue] of Object.entries(node)) {
    const nextPath = [...pathSegments, childKey];

    if (schemaDirectKeywords.has(childKey)) {
      walkSchema(childValue, node, childKey, nextPath, visitor, ancestors);
      continue;
    }

    if (schemaArrayKeywords.has(childKey) && Array.isArray(childValue)) {
      childValue.forEach((entry, index) => {
        walkSchema(
          entry,
          childValue,
          index,
          [...nextPath, index],
          visitor,
          ancestors
        );
      });
      continue;
    }

    if (schemaMapKeywords.has(childKey) && isSchemaObject(childValue)) {
      for (const [name, schema] of Object.entries(childValue)) {
        walkSchema(
          schema,
          childValue,
          name,
          [...nextPath, name],
          visitor,
          ancestors
        );
      }
      continue;
    }

    if (childKey === 'dependencies' && isSchemaObject(childValue)) {
      for (const [dependencyName, dependency] of Object.entries(childValue)) {
        if (isObject(dependency) || typeof dependency === 'boolean') {
          walkSchema(
            dependency,
            childValue,
            dependencyName,
            [...nextPath, dependencyName],
            visitor,
            ancestors
          );
        }
      }
    }
  }

  ancestors.delete(node);
}

function registerSchemasFromSchemaTree(
  rootSchema: Record<string, any>,
  state: SchemaComponentState
): void {
  walkSchema(rootSchema, undefined, undefined, [], schemaNode => {
    if (!isSchemaObject(schemaNode) || isReferenceObject(schemaNode)) {
      return;
    }

    const origin = getOrigin(schemaNode);
    if (origin && isExternalReference(origin)) {
      registerSchema(schemaNode, origin, state, origin);
    }
  });
}

async function loadSchemaFromFile(originPath: string): Promise<Record<string, any> | undefined> {
  const absoluteFilePath = path.resolve(process.cwd(), originPath);
  if (!existsSync(absoluteFilePath)) {
    return;
  }

  const fileContent = readFileSync(absoluteFilePath, 'utf-8');
  const parsedSchema = yaml.load(fileContent);
  if (!isObject(parsedSchema) && !Array.isArray(parsedSchema)) {
    return;
  }

  const previousDirectory = process.cwd();
  process.chdir(path.dirname(absoluteFilePath));

  try {
    const dereferencedSchema = (await $RefParser.dereference(parsedSchema as object, {
      dereference: {
        circular: true,
        onDereference: (ref: string, value: any) => {
          if (isObject(value)) {
            value[X_ORIGIN_KEY] = ref;
          }
        },
      },
    })) as Record<string, any>;

    dereferencedSchema[X_ORIGIN_KEY] = originPath;
    return dereferencedSchema;
  } finally {
    process.chdir(previousDirectory);
  }
}

async function ensureSchemaForDiscriminatorMapping(
  mappingRef: string,
  schemaOrigin: string | undefined,
  state: SchemaComponentState
): Promise<string | undefined> {
  const knownComponentName = resolveSchemaNameByExternalRef(mappingRef, state);
  if (knownComponentName) {
    return knownComponentName;
  }

  const resolvedOriginPath = getResolvedMappingOriginPath(
    mappingRef,
    schemaOrigin,
    state
  );
  if (!resolvedOriginPath) {
    return;
  }

  if (state.attemptedSchemaLoads.has(resolvedOriginPath)) {
    return resolveSchemaNameByExternalRef(mappingRef, state);
  }

  state.attemptedSchemaLoads.add(resolvedOriginPath);

  const loadedSchema = await loadSchemaFromFile(resolvedOriginPath);
  if (!loadedSchema) {
    return;
  }

  const componentName = registerSchema(
    loadedSchema,
    resolvedOriginPath,
    state,
    resolvedOriginPath
  );
  registerSchemasFromSchemaTree(loadedSchema, state);

  return componentName;
}

async function rewriteDiscriminatorMappings(
  state: SchemaComponentState
): Promise<void> {
  let changed = true;

  while (changed) {
    changed = false;

    for (const schema of Array.from(state.nameToSchema.values())) {
      const discriminator = schema.discriminator;
      const objectMapping =
        isSchemaObject(discriminator) && isSchemaObject(discriminator.mapping)
          ? (discriminator.mapping as Record<string, unknown>)
          : undefined;
      const extensionMapping = isSchemaObject(schema[X_DISCRIMINATOR_MAPPING_KEY])
        ? (schema[X_DISCRIMINATOR_MAPPING_KEY] as Record<string, unknown>)
        : undefined;

      const mapping = objectMapping || extensionMapping;
      if (!mapping) {
        continue;
      }

      const schemaOrigin = getOrigin(schema);

      for (const [mappingKey, mappingValue] of Object.entries(mapping)) {
        if (typeof mappingValue !== 'string') {
          continue;
        }

        if (mappingValue.startsWith(COMPONENT_SCHEMA_REF_PREFIX)) {
          continue;
        }

        if (!looksLikeFileReference(mappingValue)) {
          continue;
        }

        let mappedComponentName = resolveSchemaNameByExternalRef(mappingValue, state);
        if (!mappedComponentName) {
          mappedComponentName = await ensureSchemaForDiscriminatorMapping(
            mappingValue,
            schemaOrigin,
            state
          );
        }

        if (!mappedComponentName) {
          continue;
        }

        const componentRef = `${COMPONENT_SCHEMA_REF_PREFIX}${mappedComponentName}`;
        if (mapping[mappingKey] !== componentRef) {
          mapping[mappingKey] = componentRef;
          changed = true;
        }
      }
    }
  }
}

function normalizeSchemaDiscriminators(state: SchemaComponentState): void {
  const mergeMapping = (
    target: Record<string, unknown>,
    source: Record<string, unknown>
  ): void => {
    for (const [key, value] of Object.entries(source)) {
      target[key] = value;
    }
  };

  for (const schema of state.nameToSchema.values()) {
    walkSchema(schema, undefined, undefined, [], schemaNode => {
      if (!isSchemaObject(schemaNode)) {
        return;
      }

      const discriminator = schemaNode.discriminator;
      if (!isSchemaObject(discriminator)) {
        return;
      }

      const extensionMapping = isSchemaObject(schemaNode[X_DISCRIMINATOR_MAPPING_KEY])
        ? (schemaNode[X_DISCRIMINATOR_MAPPING_KEY] as Record<string, unknown>)
        : {};

      if (isSchemaObject(discriminator.mapping)) {
        mergeMapping(extensionMapping, discriminator.mapping as Record<string, unknown>);
      }

      if (Object.keys(extensionMapping).length > 0) {
        schemaNode[X_DISCRIMINATOR_MAPPING_KEY] = extensionMapping;
      }

      if (typeof discriminator.propertyName === 'string') {
        schemaNode.discriminator = discriminator.propertyName;
      } else {
        delete schemaNode.discriminator;
      }
    });
  }
}

function getSchemaNameWithoutNumericSuffix(name: string): string {
  return name.replace(/_\d+$/, '');
}

function hasNumericSuffix(name: string): boolean {
  return (/_\d+$/).test(name);
}

function chooseCanonicalSchemaName(left: string, right: string): string {
  const leftHasSuffix = hasNumericSuffix(left);
  const rightHasSuffix = hasNumericSuffix(right);

  if (leftHasSuffix !== rightHasSuffix) {
    return leftHasSuffix ? right : left;
  }

  if (left.length !== right.length) {
    return left.length < right.length ? left : right;
  }

  return left.localeCompare(right) <= 0 ? left : right;
}

function createSchemaAliasMap(state: SchemaComponentState): Map<string, string> {
  const groupCanonicalName = new Map<string, string>();
  const groupNames = new Map<string, string[]>();

  for (const [name, schema] of state.nameToSchema.entries()) {
    const groupKey = `${getSchemaNameWithoutNumericSuffix(name)}::${createSchemaFingerprint(schema)}`;
    const currentCanonical = groupCanonicalName.get(groupKey);

    if (currentCanonical) {
      groupCanonicalName.set(
        groupKey,
        chooseCanonicalSchemaName(currentCanonical, name)
      );
      const names = groupNames.get(groupKey) || [];
      names.push(name);
      groupNames.set(groupKey, names);
      continue;
    }

    groupCanonicalName.set(groupKey, name);
    groupNames.set(groupKey, [name]);
  }

  const aliases = new Map<string, string>();

  for (const [groupKey, names] of groupNames.entries()) {
    const canonicalName = groupCanonicalName.get(groupKey);
    if (!canonicalName) {
      continue;
    }

    for (const name of names) {
      if (name !== canonicalName) {
        aliases.set(name, canonicalName);
      }
    }
  }

  return aliases;
}

function rewriteAliasedSchemaRefs(
  node: any,
  aliases: Map<string, string>,
  seen = new Set<object>()
): void {
  if (!isObject(node)) {
    return;
  }

  if (seen.has(node)) {
    return;
  }

  seen.add(node);

  if (Array.isArray(node)) {
    node.forEach(entry => rewriteAliasedSchemaRefs(entry, aliases, seen));
    return;
  }

  if (typeof node.$ref === 'string' && node.$ref.startsWith(COMPONENT_SCHEMA_REF_PREFIX)) {
    const refName = node.$ref.slice(COMPONENT_SCHEMA_REF_PREFIX.length);
    const canonicalName = aliases.get(refName);
    if (canonicalName) {
      node.$ref = `${COMPONENT_SCHEMA_REF_PREFIX}${canonicalName}`;
    }
  }

  const discriminatorMapping = node[X_DISCRIMINATOR_MAPPING_KEY];
  if (isSchemaObject(discriminatorMapping)) {
    for (const [mappingKey, mappingValue] of Object.entries(discriminatorMapping)) {
      if (
        typeof mappingValue === 'string' &&
        mappingValue.startsWith(COMPONENT_SCHEMA_REF_PREFIX)
      ) {
        const refName = mappingValue.slice(COMPONENT_SCHEMA_REF_PREFIX.length);
        const canonicalName = aliases.get(refName);
        if (canonicalName) {
          discriminatorMapping[mappingKey] =
            `${COMPONENT_SCHEMA_REF_PREFIX}${canonicalName}`;
        }
      }
    }
  }

  for (const value of Object.values(node)) {
    rewriteAliasedSchemaRefs(value, aliases, seen);
  }
}

function applySchemaAliases(
  document: AsyncAPIObject,
  state: SchemaComponentState
): void {
  let hasAliases = true;

  while (hasAliases) {
    const aliases = createSchemaAliasMap(state);
    hasAliases = aliases.size > 0;
    if (!hasAliases) {
      break;
    }

    rewriteAliasedSchemaRefs(document, aliases);

    for (const [schemaObject, componentName] of Array.from(state.objectToName.entries())) {
      const canonicalName = aliases.get(componentName);
      if (canonicalName) {
        state.objectToName.set(schemaObject, canonicalName);
      }
    }

    for (const [originPath, componentName] of Array.from(state.fullOriginToName.entries())) {
      const canonicalName = aliases.get(componentName);
      if (canonicalName) {
        state.fullOriginToName.set(originPath, canonicalName);
      }
    }

    for (const [baseName, componentName] of Array.from(state.basenameToName.entries())) {
      const canonicalName = aliases.get(componentName);
      if (canonicalName) {
        state.basenameToName.set(baseName, canonicalName);
      }
    }

    for (const aliasName of aliases.keys()) {
      state.nameToSchema.delete(aliasName);
    }

    state.schemaSignatureToName.clear();
    for (const [name, schema] of state.nameToSchema.entries()) {
      const normalizedName = getSchemaNameFromRef(name);
      state.schemaSignatureToName.set(
        createSchemaSignature(normalizedName, schema),
        name
      );
    }
  }
}

function cloneSchemaWithRefs(
  schemaNode: any,
  componentName: string,
  state: SchemaComponentState,
  isRoot = false,
  cache = new Map<object, any>()
): any {
  if (!isObject(schemaNode)) {
    return schemaNode;
  }

  if (Array.isArray(schemaNode)) {
    return schemaNode.map(entry =>
      cloneSchemaWithRefs(entry, componentName, state, false, cache)
    );
  }

  if (isReferenceObject(schemaNode)) {
    if (schemaNode.$ref.startsWith(COMPONENT_SCHEMA_REF_PREFIX)) {
      return structuredClone(schemaNode);
    }

    if (isExternalReference(schemaNode.$ref)) {
      const mapped = resolveSchemaNameByExternalRef(schemaNode.$ref, state);
      if (mapped) {
        return createSchemaRef(mapped, schemaNode);
      }
    }

    return structuredClone(schemaNode);
  }

  const mappedName = state.objectToName.get(schemaNode);
  if (mappedName && (!isRoot || mappedName !== componentName)) {
    return createSchemaRef(mappedName, schemaNode);
  }

  const cached = cache.get(schemaNode);
  if (cached) {
    return cached;
  }

  const clone: Record<string, any> = {};
  cache.set(schemaNode, clone);

  for (const [key, value] of Object.entries(schemaNode)) {
    if (schemaDirectKeywords.has(key)) {
      clone[key] = cloneSchemaWithRefs(value, componentName, state, false, cache);
      continue;
    }

    if (schemaArrayKeywords.has(key) && Array.isArray(value)) {
      clone[key] = value.map(entry =>
        cloneSchemaWithRefs(entry, componentName, state, false, cache)
      );
      continue;
    }

    if (schemaMapKeywords.has(key) && isSchemaObject(value)) {
      const mapClone: Record<string, any> = {};
      for (const [name, schema] of Object.entries(value)) {
        mapClone[name] = cloneSchemaWithRefs(
          schema,
          componentName,
          state,
          false,
          cache
        );
      }
      clone[key] = mapClone;
      continue;
    }

    if (key === 'dependencies' && isSchemaObject(value)) {
      const dependencyClone: Record<string, any> = {};
      for (const [dependencyName, dependency] of Object.entries(value)) {
        if (isObject(dependency) || typeof dependency === 'boolean') {
          dependencyClone[dependencyName] = cloneSchemaWithRefs(
            dependency,
            componentName,
            state,
            false,
            cache
          );
        } else {
          dependencyClone[dependencyName] = structuredClone(dependency);
        }
      }
      clone[key] = dependencyClone;
      continue;
    }

    clone[key] = structuredClone(value);
  }

  return clone;
}

export async function hoistBundledSchemas(
  document: AsyncAPIObject
): Promise<AsyncAPIObject> {
  const state: SchemaComponentState = {
    objectToName: new Map<object, string>(),
    nameToSchema: new Map<string, Record<string, any>>(),
    schemaSignatureToName: new Map<string, string>(),
    fullOriginToName: new Map<string, string>(),
    basenameToName: new Map<string, string>(),
    ambiguousBasenames: new Set<string>(),
    attemptedSchemaLoads: new Set<string>(),
    fileSearchCache: new Map<string, string[]>(),
  };

  const components = (document.components = document.components || {});
  const existingSchemas =
    (components.schemas as Record<string, Record<string, any> | { $ref: string }>) ||
    {};

  for (const [name, schema] of Object.entries(existingSchemas)) {
    if (!isSchemaObject(schema) || isReferenceObject(schema)) {
      continue;
    }

    const origin = getOrigin(schema);
    registerSchema(schema, name, state, origin);
  }

  walkDocumentSchemaContexts(document, [], (schemaNode, _parent, _key, pathSegments) => {
    if (!isSchemaObject(schemaNode) || isReferenceObject(schemaNode)) {
      return;
    }

    const origin = getOrigin(schemaNode);
    if (origin && isExternalReference(origin)) {
      registerSchema(schemaNode, origin, state, origin);
      return;
    }

    if (isComponentSchemaPath(pathSegments)) {
      registerSchema(schemaNode, String(pathSegments[2]), state);
    }
  });

  walkDocumentSchemaContexts(document, [], (schemaNode, parent, key, pathSegments) => {
    if (!parent || key === undefined) {
      return;
    }

    if (isReferenceObject(schemaNode)) {
      if (schemaNode.$ref.startsWith(COMPONENT_SCHEMA_REF_PREFIX)) {
        return;
      }

      if (isExternalReference(schemaNode.$ref)) {
        const mapped = resolveSchemaNameByExternalRef(schemaNode.$ref, state);
        if (mapped) {
          parent[key] = createSchemaRef(mapped, schemaNode);
          return true;
        }
      }
      return;
    }

    if (!isSchemaObject(schemaNode)) {
      return;
    }

    const mapped = state.objectToName.get(schemaNode);
    if (!mapped || isComponentSchemaPath(pathSegments)) {
      return;
    }

    parent[key] = createSchemaRef(mapped, schemaNode);
    return true;
  });

  await rewriteDiscriminatorMappings(state);
  normalizeSchemaDiscriminators(state);
  applySchemaAliases(document, state);

  const nextSchemas: Record<string, any> = {};

  for (const [name, schema] of Object.entries(existingSchemas)) {
    if (isReferenceObject(schema)) {
      if (isExternalReference(schema.$ref)) {
        const mapped = resolveSchemaNameByExternalRef(schema.$ref, state);
        nextSchemas[name] = mapped ? createSchemaRef(mapped, schema) : schema;
      } else {
        nextSchemas[name] = structuredClone(schema);
      }
      continue;
    }

    const source = state.nameToSchema.get(name);
    nextSchemas[name] = source
      ? cloneSchemaWithRefs(source, name, state, true)
      : cloneSchemaWithRefs(schema, name, state, true);
  }

  for (const [name, schema] of state.nameToSchema.entries()) {
    if (nextSchemas[name]) {
      continue;
    }

    nextSchemas[name] = cloneSchemaWithRefs(schema, name, state, true);
  }

  components.schemas = nextSchemas;
  return document;
}

function registerChannelOrigin(
  originPath: string,
  localRef: string,
  state: ChannelRefState
): void {
  const normalized = normalizeRefPath(originPath);
  state.fullPathToRef.set(normalized, localRef);

  const baseName = path.basename(normalized);
  if (!baseName) {
    return;
  }

  const existing = state.basenameToRef.get(baseName);
  if (existing && existing !== localRef) {
    state.ambiguousBasenames.add(baseName);
  } else {
    state.basenameToRef.set(baseName, localRef);
  }
}

function escapeJsonPointerToken(token: string): string {
  return token.replace(/~/g, '~0').replace(/\//g, '~1');
}

function resolveChannelRef(
  externalRef: string,
  state: ChannelRefState
): string | undefined {
  const normalized = normalizeRefPath(externalRef);
  const byPath = state.fullPathToRef.get(normalized);

  if (byPath) {
    return byPath;
  }

  const baseName = path.basename(normalized);
  if (!baseName || state.ambiguousBasenames.has(baseName)) {
    return;
  }

  return state.basenameToRef.get(baseName);
}

function rewriteOperationChannelRef(
  candidate: any,
  state: ChannelRefState
): void {
  if (!isReferenceObject(candidate) || !isExternalReference(candidate.$ref)) {
    return;
  }

  const localRef = resolveChannelRef(candidate.$ref, state);
  if (localRef) {
    candidate.$ref = localRef;
  }
}

function rewriteOperationContainer(operationContainer: any, state: ChannelRefState): void {
  if (!isSchemaObject(operationContainer)) {
    return;
  }

  for (const operation of Object.values(operationContainer)) {
    if (!isSchemaObject(operation)) {
      continue;
    }

    rewriteOperationChannelRef(operation.channel, state);

    if (isSchemaObject(operation.reply)) {
      rewriteOperationChannelRef(operation.reply.channel, state);
    }
  }
}

export function rewriteExternalChannelRefs(document: AsyncAPIObject): AsyncAPIObject {
  const state: ChannelRefState = {
    fullPathToRef: new Map<string, string>(),
    basenameToRef: new Map<string, string>(),
    ambiguousBasenames: new Set<string>(),
  };

  if (isSchemaObject(document.channels)) {
    for (const [channelName, channel] of Object.entries(document.channels)) {
      if (!isSchemaObject(channel)) {
        continue;
      }

      const origin = getOrigin(channel);
      if (!origin || !isExternalReference(origin)) {
        continue;
      }

      registerChannelOrigin(
        origin,
        `#/channels/${escapeJsonPointerToken(channelName)}`,
        state
      );
    }
  }

  const componentChannels = isSchemaObject(document.components)
    ? document.components.channels
    : undefined;

  if (isSchemaObject(componentChannels)) {
    for (const [channelName, channel] of Object.entries(componentChannels)) {
      if (!isSchemaObject(channel)) {
        continue;
      }

      const origin = getOrigin(channel);
      if (!origin || !isExternalReference(origin)) {
        continue;
      }

      registerChannelOrigin(
        origin,
        `#/components/channels/${escapeJsonPointerToken(channelName)}`,
        state
      );
    }
  }

  rewriteOperationContainer((document as Record<string, any>).operations, state);

  if (isSchemaObject(document.components)) {
    rewriteOperationContainer(
      (document.components as Record<string, any>).operations,
      state
    );
  }

  return document;
}

export function stripXOrigin(document: AsyncAPIObject): AsyncAPIObject {
  const seen = new Set<object>();

  function walk(node: any): void {
    if (!isObject(node)) {
      return;
    }

    if (seen.has(node)) {
      return;
    }

    seen.add(node);

    if (Array.isArray(node)) {
      node.forEach(item => walk(item));
      return;
    }

    delete (node as Record<string, any>)[X_ORIGIN_KEY];

    for (const value of Object.values(node)) {
      walk(value);
    }
  }

  walk(document);
  return document;
}
