using Sitecore.Framework.Publishing.PublishJobQueue.Handlers;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using Sitecore.Framework.Publishing.Data;
using Sitecore.Framework.Publishing.ManifestCalculation;
using Sitecore.Framework.Publishing.PublisherOperation;
using Sitecore.Framework.Publishing.PublishJobQueue;
using System.Threading;
using Sitecore.Framework.Publishing;
using Sitecore.Framework.Publishing.ItemIndex;
using Sitecore.Framework.Publishing.Item;
using Sitecore.Framework.Publishing.Repository;
using Sitecore.Framework.Publishing.ContentTesting;
using Sitecore.Framework.Publishing.Workflow;
using Sitecore.Framework.Publishing.Manifest;
using Sitecore.Framework.Publishing.DataPromotion;
using Microsoft.Extensions.Logging;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.Configuration;

namespace Sitecore.Support.Framework.Publishing.PublishJobQueue.Handlers
{
  public class TreePublishHandler : BaseHandler
  {
    public TreePublishHandler(
            IRequiredPublishFieldsResolver requiredPublishFieldsResolver,
            IPublisherOperationService publisherOpsService,
            IDataStoreFactory dataStoreFactory,
            IRepositoryFactory<IMediaRepository> mediaRepositoryFactory,
            IRepositoryFactory<IItemIndexRepository> targetIndexRepositoryFactory,
            IRepositoryFactory<IItemRepository> itemRepositoryFactory,
            IRepositoryFactory<IItemRelationshipRepository> itemRelationshipRepoFactory,
            IRepositoryFactory<IItemNodeRepository> itemNodeRepositoryFactory,
            IRepositoryFactory<ITemplateGraphRepository> templateGraphRepositoryFactory,
            IRepositoryFactory<IIndexableItemRepository> indexablePublishTargetRepositoryFactory,
            IRepositoryFactory<IWorkflowStateRepository> workflowRepositoryFactory,
            IRepositoryFactory<ITestableContentRepository> testableContentRepositoryFactory,
            IRepositoryFactory<IManifestRepository> manifestRepositoryFactory,
            IPromotionCoordinator promoterCoordinator,
            Sitecore.Framework.Eventing.IEventRegistry eventRegistry,
            ILoggerFactory loggerFactory,
            IApplicationLifetime applicationLifetime,
            PublishJobHandlerOptions options = null) : base(
                requiredPublishFieldsResolver,
                publisherOpsService,
                dataStoreFactory,
                mediaRepositoryFactory,
                targetIndexRepositoryFactory,
                itemRepositoryFactory,
                itemRelationshipRepoFactory,
                itemNodeRepositoryFactory,
                templateGraphRepositoryFactory,
                indexablePublishTargetRepositoryFactory,
                workflowRepositoryFactory,
                testableContentRepositoryFactory,
                manifestRepositoryFactory,
                promoterCoordinator,
                eventRegistry,
                loggerFactory,
                applicationLifetime,
                options ?? new PublishJobHandlerOptions())
    {

    }

    public TreePublishHandler(
        IRequiredPublishFieldsResolver requiredPublishFieldsResolver,
        IPublisherOperationService publisherOpsService,
        IDataStoreFactory dataStoreFactory,
        IRepositoryFactory<IMediaRepository> mediaRepositoryFactory,
        IRepositoryFactory<IItemIndexRepository> targetIndexRepositoryFactory,
        IRepositoryFactory<IItemRepository> itemRepositoryFactory,
        IRepositoryFactory<IItemRelationshipRepository> itemRelationshipRepoFactory,
        IRepositoryFactory<IItemNodeRepository> itemNodeRepositoryFactory,
        IRepositoryFactory<ITemplateGraphRepository> templateGraphRepositoryFactory,
        IRepositoryFactory<IIndexableItemRepository> indexablePublishTargetRepositoryFactory,
        IRepositoryFactory<IWorkflowStateRepository> workflowRepositoryFactory,
        IRepositoryFactory<ITestableContentRepository> testableContentRepositoryFactory,
        IRepositoryFactory<IManifestRepository> manifestRepositoryFactory,
        IPromotionCoordinator promoterCoordinator,
        Sitecore.Framework.Eventing.IEventRegistry eventRegistry,
        ILoggerFactory loggerFactory,
        IApplicationLifetime applicationLifetime,
        IConfiguration config) : this(
            requiredPublishFieldsResolver,
            publisherOpsService,
            dataStoreFactory,
            mediaRepositoryFactory,
            targetIndexRepositoryFactory,
            itemRepositoryFactory,
            itemRelationshipRepoFactory,
            itemNodeRepositoryFactory,
            templateGraphRepositoryFactory,
            indexablePublishTargetRepositoryFactory,
            workflowRepositoryFactory,
            testableContentRepositoryFactory,
            manifestRepositoryFactory,
            promoterCoordinator,
            eventRegistry,
            loggerFactory,
            applicationLifetime,
            config.As<PublishJobHandlerOptions>())
    { }

    #region Factories

    protected override ISourceObservable<CandidateValidationContext> CreatePublishSourceStream(
        DateTime started,
        PublishOptions options,
        IPublishCandidateSource publishSourceRepository,
        IPublishValidator validator,
        IPublisherOperationService publisherOperationService,
        CancellationTokenSource errorSource)
    {
      var startNode = publishSourceRepository.GetNode(options.ItemId.Value).Result;

      if (startNode == null)
        throw new ArgumentNullException($"The publish could not be performed from a start item that doesn't exist : {options.ItemId.Value}.");

      var parentNode = startNode.ParentId != null ?
          publishSourceRepository.GetNode(startNode.ParentId.Value).Result :
          startNode;

      ISourceObservable<CandidateValidationContext> publishSourceStream = new TreeNodeSourceProducer(
          publishSourceRepository,
          startNode,
          validator,
          options.Descendants,
          _options.SourceTreeReaderBatchSize,
          errorSource,
          _loggerFactory.CreateLogger<TreeNodeSourceProducer>());

      if (options.GetItemBucketsEnabled() && parentNode.Node.TemplateId == options.GetBucketTemplateId())
      {
        publishSourceStream = new BucketNodeSourceProducer(
            publishSourceStream,
            publishSourceRepository,
            startNode,
            options.GetBucketTemplateId(),
            errorSource,
            _loggerFactory.CreateLogger<BucketNodeSourceProducer>());
      }

      publishSourceStream = new DeletedNodesSourceProducer(
          publishSourceStream,
          started,
          options.Languages,
          options.Targets,
          new string[] { options.GetPublishType() },
          publisherOperationService,
          _options.UnpublishedOperationsLoadingBatchSize,
          errorSource,
          _loggerFactory.CreateLogger<DeletedNodesSourceProducer>(),
          op => op.Path.Ancestors.Contains(options.ItemId.Value));

      return publishSourceStream;
    }

    protected override IObservable<CandidateValidationTargetContext> CreateTargetProcessingStream(
        DateTime started,
        IPublishCandidateSource publishSourceRepository,
        IPublishValidator validator,
        PublishOptions jobOptions,
        IObservable<CandidateValidationContext> publishStream,
        IItemIndexService targetIndex,
        ITestableContentRepository testableContentRepository,
        IMediaRepository targetMediaRepository,
        IRequiredPublishFieldsResolver requiredPublishFieldsResolver,
        CancellationTokenSource errorSource,
        Guid targetId)
    {
      // Source items - Create target publish stream -> PublishCandidateTargetContext
      IPublishCandidateTargetValidator parentValidator = null;
      if (jobOptions.GetItemBucketsEnabled())
      {
        parentValidator = new PublishTargetBucketParentValidator(publishSourceRepository, targetIndex, jobOptions.GetBucketTemplateId());
      }
      else
      {
        parentValidator = new PublishTargetParentValidator(publishSourceRepository, targetIndex);
      }

      publishStream = new Sitecore.Framework.Publishing.ManifestCalculation.CandidatesParentValidationTargetProducer(
          publishStream,
          parentValidator,
          errorSource,
          _loggerFactory.CreateLogger<CandidatesParentValidationTargetProducer>());

      return base.CreateTargetProcessingStream(
          started,
          publishSourceRepository,
          validator,
          jobOptions,
          publishStream,
          targetIndex,
          testableContentRepository,
          targetMediaRepository,
          requiredPublishFieldsResolver,
          errorSource,
          targetId);
    }

    #endregion

    public override bool CanHandle(PublishJob job, IDataStore from, IEnumerable<IDataStore> to) => job.Options.ItemId.HasValue;
  }
}