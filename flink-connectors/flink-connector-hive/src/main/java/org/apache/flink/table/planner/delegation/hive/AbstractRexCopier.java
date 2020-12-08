package org.apache.flink.table.planner.delegation.hive;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCorrelVariable;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexLocalRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexRangeRef;
import org.apache.calcite.rex.RexShuttle;

/**
 * Copy of Calcite RexCopier because it's package-private.
 */
public abstract class AbstractRexCopier extends RexShuttle {

	//~ Instance fields --------------------------------------------------------

	protected final RexBuilder builder;

	//~ Constructors -----------------------------------------------------------

	/**
	 * Creates a RexCopier.
	 *
	 * @param builder Builder
	 */
	AbstractRexCopier(RexBuilder builder) {
		this.builder = builder;
	}

	//~ Methods ----------------------------------------------------------------

	protected RelDataType copy(RelDataType type) {
		return builder.getTypeFactory().copyType(type);
	}

	@Override
	public RexNode visitCorrelVariable(RexCorrelVariable variable) {
		return builder.makeCorrel(copy(variable.getType()), variable.id);
	}

	@Override
	public RexNode visitFieldAccess(RexFieldAccess fieldAccess) {
		return builder.makeFieldAccess(fieldAccess.getReferenceExpr().accept(this),
				fieldAccess.getField().getIndex());
	}

	@Override
	public RexNode visitInputRef(RexInputRef inputRef) {
		return builder.makeInputRef(copy(inputRef.getType()), inputRef.getIndex());
	}

	@Override
	public RexNode visitLocalRef(RexLocalRef localRef) {
		return new RexLocalRef(localRef.getIndex(), copy(localRef.getType()));
	}

	@Override
	public RexNode visitLiteral(RexLiteral literal) {
		return super.visitLiteral(literal);
	}

	@Override
	public RexNode visitDynamicParam(RexDynamicParam dynamicParam) {
		return builder.makeDynamicParam(copy(dynamicParam.getType()),
				dynamicParam.getIndex());
	}

	@Override
	public RexNode visitRangeRef(RexRangeRef rangeRef) {
		return builder.makeRangeReference(copy(rangeRef.getType()),
				rangeRef.getOffset(), false);
	}
}
