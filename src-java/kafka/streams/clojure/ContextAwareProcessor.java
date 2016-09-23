package kafka.streams.clojure;

import clojure.lang.IFn;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;

/*
 * Create a processor which is instantiated with three handlers for process, punctuate and close supplying
 * the processor context with all three.
 */
public class ContextAwareProcessor implements Processor {

    private ProcessorContext processorContext;
    private IFn process;
    private IFn punctuate;
    private IFn close;

    public ContextAwareProcessor(IFn process, IFn punctuate, IFn close) {
        this.process = process;
        this.punctuate = punctuate;
        this.close = close;
    }

    @Override
    public void init(ProcessorContext processorContext) {
        this.processorContext = processorContext;
    }

    @Override
    public void process(Object k, Object v) {
        process.invoke(k, v, getProcessorContext());
    }

    @Override
    public void punctuate(long l) {
        punctuate.invoke(getProcessorContext());
    }

    @Override
    public void close() {
        close.invoke(getProcessorContext());
    }

    public ProcessorContext getProcessorContext() {
        return this.processorContext;
    }
}