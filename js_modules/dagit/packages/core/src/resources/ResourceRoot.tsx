import {gql, useQuery} from '@apollo/client';
import {
  Alert,
  Box,
  ButtonLink,
  CaptionMono,
  Colors,
  Group,
  Heading,
  Icon,
  MiddleTruncate,
  Mono,
  NonIdealState,
  Page,
  PageHeader,
  SplitPanelContainer,
  Subheading,
  Table,
  Tag,
  Tooltip,
} from '@dagster-io/ui';
import * as React from 'react';
import {Link, useParams, useRouteMatch} from 'react-router-dom';
import styled from 'styled-components/macro';

import {showCustomAlert} from '../app/CustomAlertProvider';
import {PYTHON_ERROR_FRAGMENT} from '../app/PythonErrorFragment';
import {useTrackPageView} from '../app/analytics';
import {useDocumentTitle} from '../hooks/useDocumentTitle';
import {RepositoryLink} from '../nav/RepositoryLink';
import {SidebarSection} from '../pipelines/SidebarComponents';
import {Loading} from '../ui/Loading';
import {repoAddressToSelector} from '../workspace/repoAddressToSelector';
import {RepoAddress} from '../workspace/types';
import {workspacePathFromAddress} from '../workspace/workspacePath';

import {ResourceTabs} from './ResourceTabs';
import {
  ResourceRootQuery,
  ResourceRootQueryVariables,
  ResourceDetailsFragment,
} from './types/ResourceRoot.types';

interface Props {
  repoAddress: RepoAddress;
}

const remapName = (inName: string): string => {
  if (inName === 'StringSourceType') {
    return 'String';
  } else if (inName === 'IntSourceType') {
    return 'Int';
  } else if (inName === 'BoolSourceType') {
    return 'Bool';
  }
  return inName;
};

const succinctType = (resourceType: string | undefined): string | null => {
  return resourceType?.split('.').pop() || null;
};

const resourceDisplayName = (
  resource: undefined | {name: string; resourceType: string},
): string | null => {
  if (!resource) {
    return null;
  }
  return resource.name.startsWith('_nested_')
    ? succinctType(resource?.resourceType)
    : resource.name;
};

export const ResourceRoot: React.FC<Props> = (props) => {
  useTrackPageView();

  const {repoAddress} = props;
  const {resourceName} = useParams<{resourceName: string}>();

  useDocumentTitle(`Resource: ${resourceName}`);

  const resourceSelector = {
    ...repoAddressToSelector(repoAddress),
    resourceName,
  };
  const queryResult = useQuery<ResourceRootQuery, ResourceRootQueryVariables>(RESOURCE_ROOT_QUERY, {
    variables: {
      resourceSelector,
    },
  });

  const displayName =
    (queryResult.data?.resourceDetailsOrError.__typename === 'ResourceDetails' &&
      resourceDisplayName(queryResult.data?.resourceDetailsOrError)) ||
    resourceName;

  const numParentResources =
    queryResult.data?.resourceDetailsOrError.__typename === 'ResourceDetails'
      ? queryResult.data?.resourceDetailsOrError.parentResources.length
      : 0;

  const tab = useRouteMatch<{tab?: string; selector: string}>([
    '/locations/:repoPath/resources/:name/:tab?',
  ])?.params.tab;

  return (
    <Page style={{height: '100%', overflow: 'hidden'}}>
      <PageHeader
        title={<Heading>{displayName}</Heading>}
        tabs={
          <ResourceTabs
            repoAddress={repoAddress}
            resourceName={resourceName}
            numParentResources={numParentResources}
          />
        }
      />
      <Loading queryResult={queryResult} allowStaleData={true}>
        {({resourceDetailsOrError}) => {
          if (resourceDetailsOrError.__typename !== 'ResourceDetails') {
            let message: string | null = null;
            if (resourceDetailsOrError.__typename === 'PythonError') {
              message = resourceDetailsOrError.message;
            }

            return (
              <Alert
                intent="warning"
                title={
                  <Group direction="row" spacing={4}>
                    <div>Could not load resource.</div>
                    {message && (
                      <ButtonLink
                        color={Colors.Link}
                        underline="always"
                        onClick={() => {
                          showCustomAlert({
                            title: 'Python error',
                            body: message,
                          });
                        }}
                      >
                        View error
                      </ButtonLink>
                    )}
                  </Group>
                }
              />
            );
          }

          const resourceTypeSuccinct = succinctType(resourceDetailsOrError.resourceType);

          return (
            <div style={{height: '100%', display: 'flex'}}>
              <SplitPanelContainer
                identifier="explorer"
                firstInitialPercent={50}
                firstMinSize={400}
                first={
                  <div style={{overflowY: 'scroll'}}>
                    {tab === 'uses' ? (
                      <ResourceUses
                        resourceDetails={resourceDetailsOrError}
                        repoAddress={repoAddress}
                      />
                    ) : (
                      <ResourceConfig
                        resourceDetails={resourceDetailsOrError}
                        repoAddress={repoAddress}
                      />
                    )}
                  </div>
                }
                second={
                  <RightInfoPanel>
                    <RightInfoPanelContent>
                      <Box
                        flex={{gap: 4, direction: 'column'}}
                        margin={{left: 24, right: 12, vertical: 16}}
                      >
                        <Heading>{displayName}</Heading>

                        <Tooltip content={resourceDetailsOrError.resourceType || ''}>
                          <Mono>{resourceTypeSuccinct}</Mono>
                        </Tooltip>
                      </Box>

                      <SidebarSection title="Definition">
                        <Box padding={{vertical: 16, horizontal: 24}}>
                          <Tag icon="resource">
                            Resource in{' '}
                            <RepositoryLink repoAddress={repoAddress} showRefresh={false} />
                          </Tag>
                        </Box>
                      </SidebarSection>
                      {resourceDetailsOrError.description ? (
                        <SidebarSection title="Description">
                          <Box padding={{vertical: 16, horizontal: 24}}>
                            {resourceDetailsOrError.description}
                          </Box>
                        </SidebarSection>
                      ) : null}
                    </RightInfoPanelContent>
                  </RightInfoPanel>
                }
              />
            </div>
          );
        }}
      </Loading>
    </Page>
  );
};

const ResourceConfig: React.FC<{
  resourceDetails: ResourceDetailsFragment;
  repoAddress: RepoAddress;
}> = (props) => {
  const {resourceDetails, repoAddress} = props;

  const configuredValues = Object.fromEntries(
    resourceDetails.configuredValues.map((cv) => [cv.key, {value: cv.value, type: cv.type}]),
  );
  const nestedResources = Object.fromEntries(
    resourceDetails.nestedResources.map((nr) => [nr.name, nr.resource]),
  );

  return (
    <>
      {Object.keys(nestedResources).length > 0 && (
        <Box>
          <SectionHeader>
            <Subheading>Resource dependencies</Subheading>
          </SectionHeader>
          <Table>
            <thead>
              <tr>
                <th style={{width: 120}}>Key</th>
                <th style={{width: 180}}>Resource</th>
              </tr>
            </thead>
            <tbody>
              {Object.keys(nestedResources).map((key) => {
                return (
                  <tr key={key}>
                    <td>
                      <Box flex={{direction: 'column', gap: 4, alignItems: 'flex-start'}}>
                        <strong>{key}</strong>
                      </Box>
                    </td>
                    <td colSpan={2}>
                      <ResourceEntry
                        url={workspacePathFromAddress(
                          repoAddress,
                          `/resources/${nestedResources[key].name}`,
                        )}
                        name={resourceDisplayName(nestedResources[key]) || ''}
                        description={nestedResources[key].description || undefined}
                      />
                    </td>
                  </tr>
                );
              })}
            </tbody>
          </Table>
        </Box>
      )}
      <Box>
        <SectionHeader>
          <Subheading>Configuration</Subheading>
        </SectionHeader>
        <Table>
          <thead>
            <tr>
              <th style={{width: 120}}>Key</th>
              <th style={{width: 90}}>Type</th>
              <th style={{width: 90}}>Value</th>
            </tr>
          </thead>
          <tbody>
            {resourceDetails.configFields.length === 0 ? (
              <tr>
                <td colSpan={3}>
                  <Box padding={{vertical: 8}}>
                    <NonIdealState
                      icon="search"
                      title="No configuration"
                      description="This resource has no configuration fields."
                    />
                  </Box>
                </td>
              </tr>
            ) : (
              resourceDetails.configFields.map((field) => {
                const defaultValue = field.defaultValueAsJson;
                const type =
                  field.name in configuredValues ? configuredValues[field.name].type : null;
                const actualValue =
                  field.name in configuredValues
                    ? configuredValues[field.name].value
                    : defaultValue;

                const isDefault = type === 'VALUE' && defaultValue === actualValue;
                return (
                  <tr key={field.name}>
                    <td>
                      <Box flex={{direction: 'column', gap: 4, alignItems: 'flex-start'}}>
                        <strong>{field.name}</strong>
                        <div style={{fontSize: 12, color: Colors.Gray700}}>{field.description}</div>
                      </Box>
                    </td>
                    <td>{remapName(field.configTypeKey)}</td>
                    <td>
                      <Box flex={{direction: 'row', justifyContent: 'space-between'}}>
                        <Tooltip content={<>Default: {defaultValue}</>} canShow={!isDefault}>
                          {type === 'ENV_VAR' ? <Tag>{actualValue}</Tag> : actualValue}
                        </Tooltip>
                        {isDefault && <Tag>Default</Tag>}
                        {type === 'ENV_VAR' && <Tag intent="success">Env var</Tag>}
                      </Box>
                    </td>
                  </tr>
                );
              })
            )}
          </tbody>
        </Table>
      </Box>
    </>
  );
};

const ResourceUses: React.FC<{
  resourceDetails: ResourceDetailsFragment;
  repoAddress: RepoAddress;
}> = (props) => {
  const {resourceDetails, repoAddress} = props;

  const parentResources = resourceDetails.parentResources.map((nr) => nr.resource);
  return (
    <>
      <Box>
        <SectionHeader>
          <Subheading>Parent resources</Subheading>
        </SectionHeader>
        <Table>
          <thead>
            <tr>
              <th>Resource</th>
            </tr>
          </thead>
          <tbody>
            {parentResources.map((resource) => {
              return (
                <tr key={resource.name}>
                  <td colSpan={2}>
                    <ResourceEntry
                      url={workspacePathFromAddress(repoAddress, `/resources/${resource.name}`)}
                      name={resourceDisplayName(resource) || ''}
                      description={resource.description || undefined}
                    />
                  </td>
                </tr>
              );
            })}
          </tbody>
        </Table>
      </Box>
    </>
  );
};

export const ResourceEntry: React.FC<{
  name: string;
  url: string;
  description?: string;
}> = (props) => {
  const {url, name, description} = props;

  return (
    <Box flex={{direction: 'column'}}>
      <Box
        flex={{direction: 'row', alignItems: 'center', display: 'inline-flex', gap: 4}}
        style={{maxWidth: '100%'}}
      >
        <Icon name="resource" color={Colors.Blue700} />
        <div style={{maxWidth: '100%', whiteSpace: 'nowrap', fontWeight: 500}}>
          <Link to={url} style={{overflow: 'hidden'}}>
            <MiddleTruncate text={name} />
          </Link>
        </div>
      </Box>
      <CaptionMono>{description}</CaptionMono>
    </Box>
  );
};

export const RightInfoPanel = styled.div`
  position: relative;

  height: 100%;
  min-height: 0;
  overflow: hidden;
  display: flex;
  flex-direction: column;
  background: ${Colors.White};
`;

export const RightInfoPanelContent = styled.div`
  flex: 1;
  overflow-y: auto;
`;
export const RESOURCE_DETAILS_FRAGMENT = gql`
  fragment ResourceDetailsFragment on ResourceDetails {
    name
    description
    configFields {
      name
      description
      configTypeKey
      isRequired
      defaultValueAsJson
    }
    configuredValues {
      key
      value
    }
    nestedResources {
      name
      resource {
        name
        resourceType
        description
        configFields {
          name
          description
          configTypeKey
          isRequired
          defaultValueAsJson
        }
        configuredValues {
          key
          value
          type
        }
        nestedResources {
          name
          resource {
            name
            resourceType
          }
        }
      }
    }
    parentResources {
      name
      resource {
        name
        resourceType
        description
      }
    }
    resourceType
  }
`;
const RESOURCE_ROOT_QUERY = gql`
  query ResourceRootQuery($resourceSelector: ResourceSelector!) {
    resourceDetailsOrError(resourceSelector: $resourceSelector) {
      ...ResourceDetailsFragment
      ...PythonErrorFragment
    }
  }
  ${RESOURCE_DETAILS_FRAGMENT}
  ${PYTHON_ERROR_FRAGMENT}
`;

const SECTION_HEADER_HEIGHT = 48;
const SectionHeader = styled.div`
  background-color: ${Colors.Gray50};
  border: 0;
  box-shadow: inset 0px -1px 0 ${Colors.KeylineGray}, inset 0px 1px 0 ${Colors.KeylineGray};
  cursor: pointer;
  display: block;
  width: 100%;
  margin: 0;
  height: ${SECTION_HEADER_HEIGHT}px;
  line-height: ${SECTION_HEADER_HEIGHT}px;
  text-align: left;
  padding-left: 24px;
  font-weight: bold;
`;
