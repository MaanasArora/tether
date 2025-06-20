import { Handle, Position } from '@xyflow/react';
import React from 'react';

const TableDomainNode = ({
  positionAbsoluteX,
  positionAbsoluteY,
  data,
}: any) => {
  const x = `${Math.round(positionAbsoluteX)}px`;
  const y = `${Math.round(positionAbsoluteY)}px`;

  if (!data || !data.columns || data.columns.length === 0) {
    return <div />;
  }

  const [tableOpen, setTableOpen] = React.useState(false);

  return (
    <div
      onClick={() => setTableOpen(!tableOpen)}
      style={{
        cursor: 'pointer',
        backgroundColor: '#f0f0f0',
        padding: '10px',
        borderRadius: '5px',
      }}>
      <strong>{data.label}</strong>
      <table
        style={{
          left: x,
          top: y,
          border: '1px solid black',
          backgroundColor: '#f9f9f9',
          padding: '10px',
          fontSize: '10px',
          borderCollapse: 'collapse',
          display: tableOpen ? 'table' : 'none',
        }}>
        <thead>
          <tr>
            <th style={{ padding: '5px', border: '1px solid #ddd' }}>
              Package
            </th>
            <th style={{ padding: '5px', border: '1px solid #ddd' }}>Column</th>
            <th style={{ padding: '5px', border: '1px solid #ddd' }}>
              Example
            </th>
          </tr>
        </thead>
        {data.columns.map((column: any, index: number) => (
          <tr key={index}>
            <td style={{ padding: '5px', border: '1px solid #ddd' }}>
              {column.dataset.package.name}
            </td>
            <td style={{ padding: '5px', border: '1px solid #ddd' }}>
              {column.name}
            </td>
            <td style={{ padding: '5px', border: '1px solid #ddd' }}>
              {column.examples
                .slice(0, 3)
                .map((example: any, exIndex: number) => (
                  <span key={exIndex}>
                    {example.value}
                    {exIndex < 2 ? ', ' : ''}
                  </span>
                ))}
            </td>
          </tr>
        ))}
      </table>

      <Handle
        type='target'
        position={Position.Top}
        style={{ top: 0, left: '50%', transform: 'translateX(-50%)' }}
      />
      <Handle
        type='source'
        position={Position.Bottom}
        style={{ bottom: 0, left: '50%', transform: 'translateX(-50%)' }}
      />
    </div>
  );
};

export { TableDomainNode };
