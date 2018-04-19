using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Sitecore.Framework.Conditions;
using Sitecore.Framework.Publishing.ManifestCalculation;
using Sitecore.DynamicSerialization;

namespace Sitecore.Support.Framework.Publishing.ManifestCalculation
{
  public class CandidatesParentValidationTargetProducer : CandidateValidationProducerBase
  {
    private readonly IObservable<CandidateValidationContext> _publishStream;
    private readonly IPublishCandidateTargetValidator _validator;
    private readonly Microsoft.Extensions.Logging.ILogger _logger;

    private readonly CancellationTokenSource _errorSource;
    private readonly CancellationToken _errorToken;

    public CandidatesParentValidationTargetProducer(
        IObservable<CandidateValidationContext> publishStream,
        IPublishCandidateTargetValidator validator,
        CancellationTokenSource errorSource,
        Microsoft.Extensions.Logging.ILogger logger)
    {
      Condition.Requires(publishStream, nameof(publishStream)).IsNotNull();
      Condition.Requires(validator, nameof(validator)).IsNotNull();
      Condition.Requires(errorSource, nameof(errorSource)).IsNotNull();
      Condition.Requires(logger, nameof(logger)).IsNotNull();

      _publishStream = publishStream;
      _validator = validator;
      _logger = logger;

      _errorSource = errorSource;
      _errorToken = errorSource.Token;

      Initialize();
    }

    private void Initialize()
    {
      // Processing on the stream can only continue if the candidate has a valid parent on the target.
      var startItemValidated = false;
      var startItemIsValid = false;
      var isRelated = _publishStream == null ? false : _publishStream.GetType().Name.Contains("RelatedNodes");

      _publishStream.Subscribe(ctx =>
      {
        try
        {
          if (!startItemValidated || isRelated)
          {
            if (!ctx.IsValid)
            {
              startItemIsValid = false;
            }
            else
            {
              var candidate = ctx.AsValid().Candidate;
              startItemIsValid = _validator.IsValid(candidate).Result;
            }

            startItemValidated = true;
          }

          if (startItemIsValid || !ctx.IsValid)
          {
            Emit(ctx);
          }
          else
          {
            EmitInvalid(ctx.Id);
          }
        }
        catch (OperationCanceledException)
        {
          if (_errorToken.IsCancellationRequested)
          {
            _logger.LogTrace("CandidatesParentValidationTargetProducer cancelled.");
            Completed();
          }
        }
        catch (Exception ex)
        {
          _logger.LogError(0, ex, $"Error in the {nameof(CandidatesParentValidationTargetProducer)}");
          Errored(ex);
          _errorSource.Cancel();
          throw;
        }
      },
      ex =>
      {
        _errorSource.Cancel();
        Errored(ex);
      },
      () =>
      {
        Completed();
      },
      _errorToken);
    }
  }
}